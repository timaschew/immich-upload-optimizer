package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sync"
	"sync/atomic"
)

var jobIdCounter atomic.Int64
var jobs sync.Map // map[string]int

func newJob(r *http.Request, w http.ResponseWriter, logger *customLogger) error {
	jobID := jobIdCounter.Add(1)
	jobLogger := newCustomLogger(logger, fmt.Sprintf("job %d: ", jobID))

	formFile, formFileHeader, err := r.FormFile(filterFormKey)
	if err != nil {
		return fmt.Errorf("unable to read file in key %s from uploaded form data: %w", filterFormKey, err)
	}
	defer r.MultipartForm.RemoveAll()
	defer formFile.Close()

	jobKey := fmt.Sprintf("\"%s\" (%s)", formFileHeader.Filename, humanReadableSize(formFileHeader.Size))
	jobLogger.Printf("download original: %s", jobKey)
	if id, exists := jobs.Load(jobKey); exists {
		http.Error(w, "IUO is already processing this file. The app is re-uploading it because it's taking too long. No workaround is possible, just kill the app and wait", http.StatusInternalServerError)
		return fmt.Errorf("a job processing this file already exists with ID: %d", id)
	}
	jobs.Store(jobKey, jobID)
	defer jobs.Delete(jobKey)

	var originalHash string
	var newHash string
	uploadFile := formFile
	uploadFilename := formFileHeader.Filename
	uploadOriginal := true

	taskProcessor, err := NewTaskProcessorFromMultipart(formFile, formFileHeader)
	if err == nil && taskProcessor != nil {
		defer taskProcessor.Close()
		taskProcessor.SetLogger(jobLogger)

		// Before transcoding, check if this original file was already processed
		// and uploaded in a previous job. Strategy:
		//   1. Compute SHA1 of the original file (cheap, no transcoding yet)
		//   2. Look up the known fake (processed) hash via the reverse map
		//   3. Ask Immich directly whether an asset with that fake hash exists
		//      - "reject" (duplicate) → already on server, skip entirely
		//      - "accept"             → not on server (e.g. deleted), proceed normally
		// This breaks the retry loop that causes UQ_assets_owner_checksum errors
		// and blocks the upload queue during long batch jobs.
		earlyOriginalHash, hashErr := SHA1(taskProcessor.OriginalFile)
		if hashErr == nil {
			mapLock.RLock()
			fakeHash, known := originalToFakeChecksum[earlyOriginalHash]
			mapLock.RUnlock()
			if known {
				alreadyUploaded, checkErr := checkAssetExistsUpstream(r, fakeHash)
				if checkErr != nil {
					jobLogger.Printf("bulk-upload-check failed (continuing normally): %v", checkErr)
				} else if alreadyUploaded {
					jobLogger.Printf("skipping already uploaded asset (confirmed by Immich): %s", jobKey)
					w.WriteHeader(http.StatusOK)
					return nil
				}
			}
		} else {
			jobLogger.Printf("early SHA1 failed (continuing normally): %v", hashErr)
		}

		// Delete multipart file before running command. Saves RAM (tmpfs)
		_ = formFile.Close()
		_ = r.MultipartForm.RemoveAll()
		if err = taskProcessor.Run(); err != nil {
			return fmt.Errorf("failed to process file in job %d: %v", jobID, err.Error())
		}
		if taskProcessor.OriginalSize <= taskProcessor.ProcessedSize {
			uploadFile = taskProcessor.OriginalFile
			_ = taskProcessor.CleanWorkDir() // Save RAM before upload (tmpfs)
		} else {
			uploadFile = taskProcessor.ProcessedFile
			uploadFilename = taskProcessor.ProcessedFilename
			uploadOriginal = false
			if originalHash, err = SHA1(taskProcessor.OriginalFile); err != nil {
				return fmt.Errorf("sha1: %w", err)
			}
			_ = taskProcessor.CleanOriginalFile() // Save RAM before upload (tmpfs)
		}
	}
	// Upload the original file or processed one if a task was found
	err = uploadUpstream(w, r, uploadFile, uploadFilename)
	if err != nil {
		jobLogger.Printf("upload upstream error: %s", err.Error())
		http.Error(w, "failed to process file, view IUO logs for more info", http.StatusInternalServerError)
	}
	if uploadOriginal {
		jobLogger.Printf("uploaded original: \"%s\" (%s)", formFileHeader.Filename, humanReadableSize(formFileHeader.Size))
	} else {
		if newHash, err = SHA1(taskProcessor.ProcessedFile); err != nil {
			return fmt.Errorf("new sha1: %w", err)
		}
		addChecksums(newHash, originalHash)
		jobLogger.Printf("uploaded: \"%s\" (%s) <- (%s) \"%s\"", taskProcessor.ProcessedFilename, humanReadableSize(taskProcessor.ProcessedSize), humanReadableSize(taskProcessor.OriginalSize), taskProcessor.OriginalFilename)
	}

	return nil
}

// checkAssetExistsUpstream calls Immich's bulk-upload-check endpoint to
// determine whether an asset with the given checksum already exists on the
// server. It reuses the auth headers from the original upload request so no
// separate API key configuration is needed.
// Returns true when Immich responds with action="reject" (i.e. duplicate).
func checkAssetExistsUpstream(r *http.Request, checksum string) (bool, error) {
	body, err := json.Marshal(map[string]any{
		"assets": []map[string]any{
			{"id": "iuo-check", "checksum": checksum},
		},
	})
	if err != nil {
		return false, fmt.Errorf("marshal bulk-upload-check body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, upstreamURL+"/api/assets/bulk-upload-check", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("create bulk-upload-check request: %w", err)
	}
	// Forward auth headers from the original request (e.g. x-api-key, Authorization)
	for _, h := range []string{"Authorization", "x-api-key", "Cookie"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := getHTTPclient().Do(req)
	if err != nil {
		return false, fmt.Errorf("bulk-upload-check request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("bulk-upload-check returned HTTP %d", resp.StatusCode)
	}

	var result struct {
		Results []struct {
			Action string `json:"action"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decode bulk-upload-check response: %w", err)
	}
	if len(result.Results) == 0 {
		return false, nil
	}
	return result.Results[0].Action == "reject", nil
}

func uploadUpstream(w http.ResponseWriter, r *http.Request, file io.ReadSeeker, name string) (err error) {
	pipeReader, pipeWriter := io.Pipe()
	multipartWriter := multipart.NewWriter(pipeWriter)
	errChan := make(chan error, 1)
	defer close(errChan)
	// Prepare chunked request, this saves A LOT of RAM compared to building the whole buffer in RAM.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer pipeWriter.Close()
		defer multipartWriter.Close()
		for key, values := range r.MultipartForm.Value {
			for _, value := range values {
				if key == "filename" {
					value = name
				}
				err = multipartWriter.WriteField(key, value)
				if err != nil {
					cancel()
					errChan <- fmt.Errorf("unable to create form data: %w", err)
					return
				}
			}
		}
		part, err := multipartWriter.CreateFormFile(filterFormKey, name)
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
	defer resp.Body.Close()
	if err != nil {
		select {
		case chErr := <-errChan:
			if err != nil {
				return fmt.Errorf("error writing data to pipe: %v: %v", err, chErr)
			}
		default:
		}
		return fmt.Errorf("unable to POST: %w", err)
	}
	// Send immich response back to client
	setHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		return fmt.Errorf("unable to forward response to client: %v", err)
	}

	return nil
}