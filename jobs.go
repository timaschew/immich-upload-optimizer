package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var jobIdCounter atomic.Int64
var jobs sync.Map     // map[string]*jobEntry  (key = deviceAssetId + "|" + lowercase-extension)
var hashJobs sync.Map // map[string]*hashEntry (key = authScope + "|" + sha1, in-flight content dedup)

type jobEntry struct {
	id           int64
	downloadDone chan struct{}
	downloadOK   bool
}

type hashEntry struct {
	id          int64
	processDone chan struct{}
	processOK   bool
}

// authScope returns a per-user string built from auth-related request headers,
// used to scope the in-flight content hash map so two different users uploading
// the same bytes don't falsely dedup against each other.
func authScope(r *http.Request) string {
	return r.Header.Get("x-api-key") + "|" + r.Header.Get("Authorization") + "|" + r.Header.Get("Cookie")
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

	// Check for duplicate job using deviceAssetId + lowercase file extension before downloading the file.
	// The extension (not the full filename) is used so that:
	//   1. Live Photo pairs (HEIC + MOV share the same deviceAssetId) aren't treated as duplicates of each other — the two halves have different extensions.
	//   2. The same asset uploaded via different iOS paths (foreground vs. background_downloader) IS treated as a duplicate, even though the multipart filenames differ ("IMG_X.MOV" vs. "<localId>_<ts>_o_IMG_X.MOV").
	// A second post-download safety net using the content SHA1 catches the rare cases where this pre-download key is not enough (different deviceAssetId, same bytes).
	deviceAssetId := ""
	if ids, ok := formValues["deviceAssetId"]; ok && len(ids) > 0 {
		deviceAssetId = ids[0]
	}
	if deviceAssetId == "" {
		// deviceAssetId is already removed in this PR
		// https://github.com/immich-app/immich/issues/27818
		// and marked here to be removed: https://github.com/immich-app/immich/blob/b414b3d32b3952eb6f655d60b91240614be14acc/mobile/lib/services/foreground_upload.service.dart#L323
		// ToDo: Need to use an alternative, because file name only is not "secure" enough
		jobLogger.Print(magenta("no deviceAssetId found in form data, using filename only as job key"))
	}
	jobKey := deviceAssetId + "|" + strings.ToLower(path.Ext(filePart.FileName()))

	// The iOS app has a bug that randomly stops the 1st upload midway, causing an "unable to save uploaded file: unexpected EOF" error
	// For this reason, we don't assume a job is a duplicate immediately and instead wait until the full asset is successfully downloaded by the existing job. Not waiting makes the app never upload the asset.
	// The app "pauses" the upload and no bandwidth is wasted while waiting because the OS slows the TCP connection (since we're not reading from it)
	jobLogger.Print(yellow("received:") + " \"" + white(filePart.FileName()) + "\" " + yellow("(deviceAssetId: %s)", jobKey))
	entry := &jobEntry{
		id:           jobID,
		downloadDone: make(chan struct{}),
	}
	for {
		existing, loaded := jobs.LoadOrStore(jobKey, entry)
		if !loaded {
			break
		}
		existingEntry := existing.(*jobEntry)
		select {
		case <-existingEntry.downloadDone:
		default:
			jobLogger.Print(yellow("waiting for job %d to finish downloading", existingEntry.id))
			select {
			case <-existingEntry.downloadDone:
			case <-r.Context().Done():
				return fmt.Errorf("job %d: request cancelled while waiting for duplicate job", jobID)
			}
		}
		if existingEntry.downloadOK {
			// Existing job downloaded successfully, this is a true duplicate
			// Hijack the connection to immediately stop the app from sending more data for this asset
			if hj, ok := w.(http.Hijacker); ok {
				conn, bufrw, err := hj.Hijack()
				if err == nil {
					_, _ = bufrw.WriteString("HTTP/1.1 409 Conflict\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nIUO is already processing this asset\r\n")
					_ = bufrw.Flush()
					if tcpConn, ok := conn.(*net.TCPConn); ok {
						tcpConn.CloseWrite()
						tcpConn.SetReadDeadline(time.Now().Add(time.Millisecond * 250))
						io.Copy(io.Discard, tcpConn)
					}
					conn.Close()
				}
			}
			return fmt.Errorf("job %d: job %d is already processing this asset", jobID, existingEntry.id)
		}
		jobLogger.Print(yellow("job %d download failed, retrying", existingEntry.id))
	}
	defer func() {
		jobs.Delete(jobKey)
		if !entry.downloadOK {
			close(entry.downloadDone)
		}
	}()

	// Acquire a processing slot BEFORE touching tmpfs. This bounds the number of
	// files simultaneously occupying TMPDIR to max_image_jobs+max_video_jobs.
	// Without this gate the iOS app's parallel upload connections each write
	// their full payload to tmpfs and only then wait for a worker slot, which
	// during bulk uploads filled the 4 GB tmpfs and produced ENOSPC errors.
	// The iOS client pauses transmission via TCP backpressure while we don't
	// read from the connection, so no bandwidth is wasted.
	fileName := filePart.FileName()
	ext := strings.ToLower(strings.TrimPrefix(path.Ext(fileName), "."))
	sem := videoSemaphore
	semName := "video"
	if slices.Contains(imageExtensions, ext) {
		sem = imageSemaphore
		semName = "image"
	}
	select {
	case sem <- struct{}{}:
	default:
		jobLogger.Print(yellow("waiting for free %s slot", semName))
		select {
		case sem <- struct{}{}:
		case <-r.Context().Done():
			return fmt.Errorf("job %d: request cancelled while waiting for %s slot", jobID, semName)
		}
	}
	defer func() { <-sem }()

	// Download original file
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
	entry.downloadOK = true
	close(entry.downloadDone)
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
	// Catches cases the pre-download key misses: e.g. the same bytes uploaded twice with a
	// different deviceAssetId, or the same deviceAssetId where the extension happens to match
	// a different in-flight file (rare). Avoids the Immich UQ_assets_owner_checksum constraint
	// failure and the orphaned files in upload/ that follow it.
	hashKey := authScope(r) + "|" + originalHash
	hashEntryNew := &hashEntry{id: jobID, processDone: make(chan struct{})}
	for {
		existing, loaded := hashJobs.LoadOrStore(hashKey, hashEntryNew)
		if !loaded {
			break
		}
		existingHashEntry := existing.(*hashEntry)
		jobLogger.Print(yellow("waiting for job %d to finish processing (same content hash)", existingHashEntry.id))
		select {
		case <-existingHashEntry.processDone:
		case <-r.Context().Done():
			return fmt.Errorf("job %d: request cancelled while waiting for duplicate hash job", jobID)
		}
		if existingHashEntry.processOK {
			// Existing job already uploaded the same content. Hijack the connection like the pre-download dedup does.
			if hj, ok := w.(http.Hijacker); ok {
				conn, bufrw, err := hj.Hijack()
				if err == nil {
					_, _ = bufrw.WriteString("HTTP/1.1 409 Conflict\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nIUO already processed an asset with the same content\r\n")
					_ = bufrw.Flush()
					if tcpConn, ok := conn.(*net.TCPConn); ok {
						tcpConn.CloseWrite()
						tcpConn.SetReadDeadline(time.Now().Add(time.Millisecond * 250))
						io.Copy(io.Discard, tcpConn)
					}
					conn.Close()
				}
			}
			return fmt.Errorf("job %d: job %d already processed an asset with the same content hash", jobID, existingHashEntry.id)
		}
		jobLogger.Print(yellow("job %d hash processing failed, retrying", existingHashEntry.id))
	}
	defer func() {
		hashJobs.Delete(hashKey)
		if !hashEntryNew.processOK {
			close(hashEntryNew.processDone)
		}
	}()

	if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("job %d: unable to seek temp file: %w", jobID, err)
	}

	uploadFile := io.ReadSeeker(tmpFile)
	uploadFilename := fileName
	uploadOriginal := true

	taskProcessor, err := NewTaskProcessor(tmpFile, fileName, fileSize, jobLogger)
	if err == nil && taskProcessor != nil {
		defer taskProcessor.Close()
		if err = taskProcessor.Run(); err != nil {
			return fmt.Errorf("job %d: failed to process file: %v", jobID, err.Error())
		}
		if taskProcessor.OriginalSize <= taskProcessor.ProcessedSize {
			uploadFile = taskProcessor.OriginalFile
			_ = taskProcessor.CleanWorkDir() // Save RAM before upload (tmpfs)
		} else {
			uploadFile = taskProcessor.ProcessedFile
			uploadFilename = taskProcessor.ProcessedFilename
			uploadOriginal = false
			_ = taskProcessor.CleanOriginalFile() // Save RAM before upload (tmpfs)
		}
	}
	// Upload the original file or processed one if a task was found
	err = uploadUpstream(w, r, uploadFile, uploadFilename, formValues)
	if err != nil {
		http.Error(w, "failed to process file, view IUO logs for more info", http.StatusConflict)
		return fmt.Errorf("job %d: upload upstream: %w", jobID, err)
	}
	if uploadOriginal {
		jobLogger.Print(greenBold("uploaded original:") + " \"" + white(fileName) + "\" " + greenBold("(%s)", humanReadableSize(fileSize)))
	} else {
		if newHash, err := SHA1(taskProcessor.ProcessedFile); err == nil {
			addChecksums(newHash, originalHash)
			jobLogger.Print(greenBold("uploaded:") + " \"" + white(taskProcessor.ProcessedFilename) + "\" " + greenBold("(%s) <- (%s)", humanReadableSize(taskProcessor.ProcessedSize), humanReadableSize(taskProcessor.OriginalSize)) + " \"" + white(taskProcessor.OriginalFilename) + "\"")
		} else {
			return fmt.Errorf("job %d: new sha1: %w", jobID, err)
		}
	}

	hashEntryNew.processOK = true
	close(hashEntryNew.processDone)

	return nil
}

func uploadUpstream(w http.ResponseWriter, r *http.Request, file io.ReadSeeker, name string, formValues map[string][]string) (err error) {
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
		return fmt.Errorf("unable to create POST request: %w", err)
	}
	req.Header = r.Header
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	// Send the request to the upstream server
	resp, err := getHTTPclient().Do(req)
	if err != nil {
		select {
		case chErr := <-errChan:
			if chErr != nil {
				return fmt.Errorf("error writing data to pipe: %v: %v", err, chErr)
			}
		default:
		}
		return fmt.Errorf("unable to POST: %w", err)
	}
	defer resp.Body.Close()
	// Send immich response back to client
	setHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		return fmt.Errorf("unable to forward response to client: %v", err)
	}

	return nil
}
