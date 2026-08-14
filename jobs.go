package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var jobIdCounter atomic.Int64
var jobs sync.Map     // map[string]*inflightEntry (key = deviceAssetId + "|" + lowercase-original-filename, only populated when the client sends a deviceAssetId)
var hashJobs sync.Map // map[string]*inflightEntry (key = authScope + "|" + sha1, in-flight content dedup)

// inflightEntry lets other requests for the same asset wait until this job reaches its checkpoint:
// the finished download for the pre-download map, the finished upload for the content hash map.
type inflightEntry struct {
	id   int64
	done chan struct{}
	ok   bool
}

// authScope returns a fingerprint of the request's credentials, used to scope the in-flight content
// hash map so two users uploading the same bytes don't dedup against each other. The credentials are
// hashed instead of concatenated so they aren't held as map keys, and the URL query is included
// because shared links authenticate through it (?key=&slug=) rather than through headers.
func authScope(r *http.Request) string {
	h := sha256.New()
	for _, v := range []string{
		r.Header.Get("x-api-key"),
		r.Header.Get("Authorization"),
		r.Header.Get("Cookie"),
		r.URL.RawQuery,
	} {
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// reject409 answers with 409 and hijacks the connection so the client stops sending the rest of the
// asset immediately instead of uploading megabytes that are going to be discarded.
func reject409(w http.ResponseWriter, message string) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		return
	}
	_, _ = bufrw.WriteString("HTTP/1.1 409 Conflict\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n" + message + "\r\n")
	_ = bufrw.Flush()
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
		tcpConn.SetReadDeadline(time.Now().Add(time.Millisecond * 250))
		io.Copy(io.Discard, tcpConn)
	}
	conn.Close()
}

func newJob(r *http.Request, w http.ResponseWriter, logger *customLogger) error {
	jobID := jobIdCounter.Add(1)
	jobLogger := newCustomLogger(logger, fmt.Sprintf("job %d: ", jobID))

	// Use streaming multipart reader to read form fields before the file. This allows checking for duplicate jobs before downloading the full asset data.
	mr, err := r.MultipartReader()
	if err != nil {
		return fmt.Errorf("job %d: unable to create multipart reader: %w", jobID, err)
	}

	formValues := make(map[string][]string)
	var filePart *multipart.Part
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("job %d: unable to read multipart part: %w", jobID, err)
		}
		if part.FileName() != "" {
			filePart = part
			break
		}
		fieldName := part.FormName()
		value, err := io.ReadAll(part)
		part.Close()
		if err != nil {
			return fmt.Errorf("job %d: unable to read form field %s: %w", jobID, fieldName, err)
		}
		formValues[fieldName] = append(formValues[fieldName], string(value))
	}
	if filePart == nil {
		return fmt.Errorf("job %d: no file found in multipart form data", jobID)
	}
	defer filePart.Close()

	// Check for duplicate job using deviceAssetId + original filename before downloading the file.
	// The name is taken from the "filename" form field and only falls back to the multipart filename, because the two iOS upload paths disagree on the latter:
	//   - foreground sends the real name as the multipart filename ("IMG_X.MOV") and no "filename" field
	//   - background_downloader sends the PhotoKit temp basename ("<localId>_<ts>_o_IMG_X.MOV") as the multipart filename, but the real name in the "filename" field
	// Using the declared original name makes both paths produce the same key, while Live Photo halves (HEIC + MOV share the same deviceAssetId) keep different keys.
	deviceAssetId := ""
	if ids, ok := formValues["deviceAssetId"]; ok && len(ids) > 0 {
		deviceAssetId = ids[0]
	}
	uploadName := filePart.FileName()
	if names, ok := formValues["filename"]; ok && len(names) > 0 {
		// RFC 7578 §4.2 requires that directory information in a filename is not used. Part.FileName() already applies this to the multipart name, a raw form field has no such protection.
		// path.Base("") returns ".", so this also covers an empty field value.
		if base := path.Base(names[0]); base != "." && base != "/" && base != ".." {
			uploadName = base
		}
	}
	jobKey := deviceAssetId + "|" + strings.ToLower(uploadName)

	// This pre-download map only exists to suppress retries of the same request, which is an iOS-only problem (see the EOF comment below).
	// Immich v3 dropped deviceAssetId from the upload DTO, so the web UI and the CLI no longer send it and there is no stable per-asset identity to key on.
	// Keying on the filename alone would make concurrent uploads of different files collide (web uses concurrency 2, the CLI cpus-1), so those clients get no pre-download dedup at all.
	// They don't have the iOS retry bug, and the post-download content hash check still keeps duplicates away from Immich.
	// ToDo: the mobile app stops sending deviceAssetId in Immich v4.0 (https://github.com/immich-app/immich/issues/27818). iOS then loses this protection and needs a replacement key.
	preDownloadDedup := deviceAssetId != ""
	if !preDownloadDedup {
		jobLogger.Print(magenta("no deviceAssetId found in form data, skipping pre-download dedup (content hash check still applies)"))
	}

	// The iOS app has a bug that randomly stops the 1st upload midway, causing an "unable to save uploaded file: unexpected EOF" error
	// For this reason, we don't assume a job is a duplicate immediately and instead wait until the full asset is successfully downloaded by the existing job. Not waiting makes the app never upload the asset.
	// The app "pauses" the upload and no bandwidth is wasted while waiting because the OS slows the TCP connection (since we're not reading from it)
	jobLogger.Print(yellow("received:") + " \"" + white(filePart.FileName()) + "\" " + yellow("(job key: %s)", jobKey))
	var entry *inflightEntry
	if preDownloadDedup {
		entry = &inflightEntry{id: jobID, done: make(chan struct{})}
		for {
			existing, loaded := jobs.LoadOrStore(jobKey, entry)
			if !loaded {
				break
			}
			existingEntry := existing.(*inflightEntry)
			select {
			case <-existingEntry.done:
			default:
				jobLogger.Print(yellow("waiting for job %d to finish downloading", existingEntry.id))
				select {
				case <-existingEntry.done:
				case <-r.Context().Done():
					return fmt.Errorf("job %d: request cancelled while waiting for duplicate job", jobID)
				}
			}
			if existingEntry.ok {
				// Existing job downloaded successfully, this is a true duplicate
				reject409(w, "IUO is already processing this asset")
				return fmt.Errorf("job %d: job %d is already processing this asset", jobID, existingEntry.id)
			}
			jobLogger.Print(yellow("job %d download failed, retrying", existingEntry.id))
		}
		defer func() {
			jobs.Delete(jobKey)
			if !entry.ok {
				close(entry.done)
			}
		}()
	}

	// Download original file
	// Immich stores the "filename" form field as the asset's originalFileName ("originalFileName: dto.filename || file.originalName"),
	// and uploadUpstream rewrites that field with the name passed to it. Use the name the client declared as the original, otherwise
	// iOS background uploads end up stored under their PhotoKit temp name (<localId>_<ts>_o_IMG_X.MOV) instead of IMG_X.MOV.
	// The extension keeps coming from the multipart filename: it describes the bytes actually received and selects the task in NewTaskProcessor.
	fileName := uploadName
	if partExt := path.Ext(filePart.FileName()); partExt != "" {
		fileName = strings.TrimSuffix(fileName, path.Ext(fileName)) + partExt
	}
	tmpFile, err := os.CreateTemp("", "upload-*"+path.Ext(fileName))
	if err != nil {
		return fmt.Errorf("job %d: unable to create temp file: %w", jobID, err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	fileSize, err := io.Copy(tmpFile, filePart)
	if err != nil {
		return fmt.Errorf("job %d: unable to save uploaded file: %w", jobID, err)
	}
	filePart.Close()
	if entry != nil {
		entry.ok = true
		close(entry.done)
	}
	jobLogger.Print(green("downloaded:") + " \"" + white(fileName) + "\" " + green("(%s)", humanReadableSize(fileSize)))
	// Read any remaining form fields after the file
	for {
		part, err := mr.NextPart()
		if err != nil {
			break
		}
		if part.FileName() == "" {
			fieldName := part.FormName()
			value, _ := io.ReadAll(part)
			part.Close()
			formValues[fieldName] = append(formValues[fieldName], string(value))
		} else {
			part.Close()
		}
	}

	// Skip this asset if the optimized version is already on the Immich server.
	// This is a safety net to guarantee no duplicate ever reaches the server if clients fail to check themselves before uploading.
	originalHash := ""
	if originalHash, err = SHA1(tmpFile); err != nil {
		return fmt.Errorf("job %d: sha1 original: %w", jobID, err)
	}
	mapLock.RLock()
	fakeHash, hasFake := originalToFakeChecksum[originalHash]
	mapLock.RUnlock()
	if hasFake {
		checkBody, _ := json.Marshal(bulkUploadCheckRequest{Assets: []bulkUploadCheckItem{{ID: fmt.Sprintf("job%d", jobID), Checksum: fakeHash}}})
		checkReq, err := http.NewRequest("POST", upstreamURL+"/api/assets/bulk-upload-check", bytes.NewReader(checkBody))
		if err == nil {
			checkReq.Header = r.Header.Clone()
			checkReq.Header.Set("Content-Type", "application/json")
			resp, err := getHTTPclient().Do(checkReq)
			if err == nil {
				var checkResp bulkUploadCheckResponse
				if err := json.NewDecoder(resp.Body).Decode(&checkResp); err == nil {
					if len(checkResp.Results) > 0 && checkResp.Results[0].Action == "reject" && checkResp.Results[0].ID == fmt.Sprintf("job%d", jobID) {
						resp.Body.Close()
						jobLogger.Print(yellow("skipped:") + " \"" + white(fileName) + "\" " + yellow("(optimized version already on the immich server)"))
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						json.NewEncoder(w).Encode(assetMediaResponse{ID: checkResp.Results[0].AssetID, Status: "duplicate"})
						return nil
					}
				}
				resp.Body.Close()
			}
		}
	}

	// Post-download safety net: dedup by content hash (per-user-scoped).
	// This is the only in-flight dedup for clients that send no deviceAssetId (Immich v3 web UI and CLI),
	// and it catches what the pre-download key cannot know: the same bytes arriving under a different
	// deviceAssetId or a different original filename. Avoids the Immich UQ_assets_owner_checksum
	// constraint failure and the orphaned files in upload/ that follow it.
	hashKey := authScope(r) + "|" + originalHash
	hashEntry := &inflightEntry{id: jobID, done: make(chan struct{})}
	for {
		existing, loaded := hashJobs.LoadOrStore(hashKey, hashEntry)
		if !loaded {
			break
		}
		existingHashEntry := existing.(*inflightEntry)
		select {
		case <-existingHashEntry.done:
		default:
			jobLogger.Print(yellow("waiting for job %d to finish processing (same content hash)", existingHashEntry.id))
			select {
			case <-existingHashEntry.done:
			case <-r.Context().Done():
				return fmt.Errorf("job %d: request cancelled while waiting for duplicate hash job", jobID)
			}
		}
		if existingHashEntry.ok {
			// Existing job already uploaded the same content
			reject409(w, "IUO already processed an asset with the same content")
			return fmt.Errorf("job %d: job %d already processed an asset with the same content hash", jobID, existingHashEntry.id)
		}
		jobLogger.Print(yellow("job %d hash processing failed, retrying", existingHashEntry.id))
	}
	defer func() {
		hashJobs.Delete(hashKey)
		if !hashEntry.ok {
			close(hashEntry.done)
		}
	}()

	if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("job %d: unable to seek temp file: %w", jobID, err)
	}

	// Android motion photos carry their video inside the JPEG. It has to be split off before any task runs:
	// the conversion would drop the video while the XMP keeps claiming it is there, which makes immich read
	// garbage at the offset it computes from that XMP (metadata.service.ts, applyMotionPhotos).
	motionVideoID := ""
	motionSplit := false
	if motionPhotoSplit {
		mp, mpErr := detectMotionPhoto(tmpFile, fileSize)
		switch {
		case mpErr != nil:
			jobLogger.Print(red("motion photo detection failed: %v", mpErr))
		case mp == nil:
			// Not a motion photo, nothing to do
		case mp.VideoOffset == 0 && mp.DeclaredLength == 0:
			// Nothing in the metadata says where a video would be, so immich can't read at a wrong offset
			// either. Leave the file untouched, it may still hold a video only the server can get at, like
			// the exif binary tag Samsung uses.
			jobLogger.Print(yellow("motion photo flag without an embedded video location, uploading unchanged"))
		case mp.VideoOffset == 0:
			// Nothing left to extract, but the flag has to go or immich reads garbage where it expects the video
			if err = patchMotionFlags(tmpFile, mp); err != nil {
				http.Error(w, "failed to process motion photo, view IUO logs for more info", http.StatusConflict)
				return fmt.Errorf("job %d: %w", jobID, err)
			}
			jobLogger.Print(yellow("motion photo announcing %s of video that isn't there, uploading the image only", humanReadableSize(mp.DeclaredLength)))
		default:
			videoFile, stillSize, splitErr := splitMotionPhoto(tmpFile, fileSize, mp)
			if splitErr != nil {
				http.Error(w, "failed to process motion photo, view IUO logs for more info", http.StatusConflict)
				return fmt.Errorf("job %d: motion photo split: %w", jobID, splitErr)
			}
			jobLogger.Print(cyan("motion photo: %s image + %s video", humanReadableSize(stillSize), humanReadableSize(mp.VideoLength)))
			motionSplit = true
			fileSize = stillSize
			// immich names the video part of a motion photo after the image, keep that convention
			videoName := strings.TrimSuffix(fileName, path.Ext(fileName)) + ".mp4"
			motionVideoID, err = processMotionVideo(r, videoFile, videoName, mp.VideoLength, formValues, jobLogger)
			if err != nil {
				http.Error(w, "failed to process motion photo, view IUO logs for more info", http.StatusConflict)
				return fmt.Errorf("job %d: motion photo video: %w", jobID, err)
			}
		}
	}
	// Undo the video upload if the still image never makes it to immich, so no orphan is left in the timeline
	cleanupMotionVideo := func() {
		if motionVideoID != "" {
			deleteAsset(r, motionVideoID, jobLogger)
		}
	}

	uploadFile, uploadFilename, taskProcessor, converted, err := runTask(tmpFile, fileName, fileSize, jobLogger)
	if taskProcessor != nil {
		defer taskProcessor.Close()
	}
	if err != nil {
		cleanupMotionVideo()
		http.Error(w, "failed to process file, view IUO logs for more info", http.StatusConflict)
		return fmt.Errorf("job %d: failed to process file: %v", jobID, err.Error())
	}

	extra := map[string]string{}
	if motionVideoID != "" {
		extra["livePhotoVideoId"] = motionVideoID
	}
	// Upload the original file or processed one if a task was found
	status, err := uploadUpstream(w, r, uploadFile, uploadFilename, formValues, extra, jobLogger)
	if err != nil {
		cleanupMotionVideo()
		http.Error(w, "failed to process file, view IUO logs for more info", http.StatusConflict)
		return fmt.Errorf("job %d: upload upstream: %w", jobID, err)
	}
	if status >= 400 {
		cleanupMotionVideo()
		return fmt.Errorf("job %d: immich rejected the upload with status %d", jobID, status)
	}
	if converted {
		newHash, hashErr := SHA1(taskProcessor.ProcessedFile)
		if hashErr != nil {
			return fmt.Errorf("job %d: new sha1: %w", jobID, hashErr)
		}
		addChecksums(newHash, originalHash)
		jobLogger.Print(greenBold("uploaded:") + " \"" + white(taskProcessor.ProcessedFilename) + "\" " + greenBold("(%s) <- (%s)", humanReadableSize(taskProcessor.ProcessedSize), humanReadableSize(taskProcessor.OriginalSize)) + " \"" + white(taskProcessor.OriginalFilename) + "\"")
	} else {
		if motionSplit {
			// The client knows the checksum of the whole .MP.jpg only. Without a mapping to the still image
			// its bulk-upload-check never matches and the photo is uploaded again on every sync.
			stillHash, hashErr := SHA1(uploadFile)
			if hashErr != nil {
				return fmt.Errorf("job %d: still sha1: %w", jobID, hashErr)
			}
			addChecksums(stillHash, originalHash)
		}
		jobLogger.Print(greenBold("uploaded original:") + " \"" + white(fileName) + "\" " + greenBold("(%s)", humanReadableSize(fileSize)))
	}

	hashEntry.ok = true
	close(hashEntry.done)

	return nil
}

// processMotionVideo runs the configured task on the video extracted from a motion photo and uploads it as its
// own asset. The temp file is consumed either way.
func processMotionVideo(r *http.Request, videoFile *os.File, name string, size int64, formValues map[string][]string, logger *customLogger) (string, error) {
	uploadFile, uploadName, taskProcessor, converted, err := runTask(videoFile, name, size, logger)
	if taskProcessor != nil {
		defer taskProcessor.Close()
	} else {
		defer func() { videoFile.Close(); _ = os.Remove(videoFile.Name()) }()
	}
	if err != nil {
		return "", fmt.Errorf("failed to process video: %w", err)
	}
	assetID, err := uploadMotionVideo(r, uploadFile, uploadName, formValues)
	if err != nil {
		return "", err
	}
	if converted {
		logger.Print(greenBold("uploaded motion video:") + " \"" + white(uploadName) + "\" " + greenBold("(%s) <- (%s)", humanReadableSize(taskProcessor.ProcessedSize), humanReadableSize(taskProcessor.OriginalSize)))
	} else {
		logger.Print(greenBold("uploaded motion video:") + " \"" + white(uploadName) + "\" " + greenBold("(%s)", humanReadableSize(size)))
	}
	return assetID, nil
}

// postAsset uploads a file to the upstream /api/assets endpoint, reusing the form fields the client sent.
// Fields in extra are added to the request and take precedence over the client's values.
func postAsset(r *http.Request, file io.ReadSeeker, name string, formValues map[string][]string, extra map[string]string) (resp *http.Response, err error) {
	pipeReader, pipeWriter := io.Pipe()
	defer pipeReader.Close()
	multipartWriter := multipart.NewWriter(pipeWriter)
	errChan := make(chan error, 1)
	// Prepare chunked request, this saves A LOT of RAM compared to building the whole buffer in RAM.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer pipeWriter.Close()
		defer multipartWriter.Close()
		for key, values := range formValues {
			if _, replaced := extra[key]; replaced {
				continue
			}
			for _, value := range values {
				if key == "filename" {
					value = name
				}
				if err := multipartWriter.WriteField(key, value); err != nil {
					cancel()
					errChan <- fmt.Errorf("unable to create form data: %w", err)
					return
				}
			}
		}
		for key, value := range extra {
			if err := multipartWriter.WriteField(key, value); err != nil {
				cancel()
				errChan <- fmt.Errorf("unable to create form data: %w", err)
				return
			}
		}
		part, err := multipartWriter.CreateFormFile("assetData", name)
		if err != nil {
			cancel()
			errChan <- fmt.Errorf("unable to create form data: %w", err)
			return
		}
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			cancel()
			errChan <- fmt.Errorf("unable to seek beginning of file: %w", err)
			return
		}
		_, err = io.Copy(part, file)
		if err != nil {
			cancel()
			errChan <- fmt.Errorf("unable to write file in form field: %w", err)
			return
		}
		err = multipartWriter.Close()
		if err != nil {
			cancel()
			errChan <- fmt.Errorf("unable to finish form data: %w", err)
			return
		}
		errChan <- nil
	}()
	req, err := http.NewRequestWithContext(ctx, "POST", upstreamURL+r.URL.String(), pipeReader)
	if err != nil {
		return nil, fmt.Errorf("unable to create POST request: %w", err)
	}
	req.Header = r.Header.Clone()
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	// Send the request to the upstream server
	resp, err = getHTTPclient().Do(req)
	if err != nil {
		select {
		case chErr := <-errChan:
			if chErr != nil {
				return nil, fmt.Errorf("error writing data to pipe: %v: %v", err, chErr)
			}
		default:
		}
		return nil, fmt.Errorf("unable to POST: %w", err)
	}
	return resp, nil
}

// uploadUpstream uploads the asset and forwards the immich response to the client. It returns the status code
// immich answered with, or 0 when the upload never made it there. A failure to forward the response to the
// client is logged but not returned: the asset is on the server at that point.
func uploadUpstream(w http.ResponseWriter, r *http.Request, file io.ReadSeeker, name string, formValues map[string][]string, extra map[string]string, logger *customLogger) (status int, err error) {
	resp, err := postAsset(r, file, name, formValues, extra)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Send immich response back to client
	setHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err = io.Copy(w, resp.Body); err != nil {
		logger.Print(red("unable to forward response to client: %v", err))
	}
	return resp.StatusCode, nil
}

// uploadMotionVideo uploads the video extracted from a motion photo as its own asset and returns its id, so the
// still image can be uploaded with a livePhotoVideoId pointing at it. The asset is created hidden right away,
// which is where immich would move it anyway once the two are linked (utils/asset.util.ts, onBeforeLink).
func uploadMotionVideo(r *http.Request, file io.ReadSeeker, name string, formValues map[string][]string) (string, error) {
	fields := make(map[string][]string, len(formValues))
	for key, values := range formValues {
		// The mobile app deliberately keeps metadata on the still image only, and any livePhotoVideoId the
		// client sent belongs to the still image as well.
		if key == "metadata" || key == "livePhotoVideoId" {
			continue
		}
		fields[key] = values
	}
	resp, err := postAsset(r, file, name, fields, map[string]string{"visibility": "hidden"})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("unable to read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("immich answered %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var media assetMediaResponse
	if err = json.Unmarshal(body, &media); err != nil {
		return "", fmt.Errorf("unable to parse response: %w", err)
	}
	if media.ID == "" {
		// A "duplicate" status still carries the id of the existing asset, which links just as well.
		return "", fmt.Errorf("no asset id in response: %s", strings.TrimSpace(string(body)))
	}
	return media.ID, nil
}

// deleteAsset removes an asset that was uploaded but couldn't be linked, so no orphaned video is left behind.
func deleteAsset(r *http.Request, assetID string, logger *customLogger) {
	body, err := json.Marshal(map[string]any{"ids": []string{assetID}, "force": true})
	if err != nil {
		logger.Print(red("unable to build delete request for asset %s: %v", assetID, err))
		return
	}
	req, err := http.NewRequest("DELETE", upstreamURL+"/api/assets", bytes.NewReader(body))
	if err != nil {
		logger.Print(red("unable to create delete request for asset %s: %v", assetID, err))
		return
	}
	req.Header = r.Header.Clone()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Del("Content-Length")
	resp, err := getHTTPclient().Do(req)
	if err != nil {
		logger.Print(red("unable to delete asset %s: %v", assetID, err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logger.Print(red("unable to delete asset %s: immich answered %d", assetID, resp.StatusCode))
		return
	}
	logger.Print(yellow("removed orphaned motion photo video asset %s", assetID))
}
