package main

import (
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

	// Step 1: Compute SHA1 of original file and check with Immich BEFORE any processing
	originalChecksum, sha1Err := SHA1(formFile)
	if sha1Err != nil {
		jobLogger.Printf("sha1 for bulk upload check failed: %s, proceeding", sha1Err.Error())
	} else {
		checksumToCheck := originalChecksum
		if fakeChecksum, found := reverseLookupChecksum(originalChecksum); found {
			checksumToCheck = fakeChecksum
		}
		alreadyExists, assetId, checkErr := checkBulkUpload(checksumToCheck, r)
		if checkErr != nil {
			jobLogger.Printf("bulk upload check failed: %s, proceeding", checkErr.Error())
		} else if alreadyExists {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":        assetId,
				"duplicate": true,
				"status":    "duplicate",
			})
			jobLogger.Printf("skipped (already in Immich): \"%s\"", formFileHeader.Filename)
			return nil
		}
	}
	// Reset file position after SHA1 computation
	if _, err = formFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("unable to seek beginning of file: %w", err)
	}

	// Step 2: Process file if a matching task exists
	var originalHash string
	uploadFile := formFile
	uploadFilename := formFileHeader.Filename
	uploadOriginal := true

	taskProcessor, err := NewTaskProcessorFromMultipart(formFile, formFileHeader)
	if err == nil && taskProcessor != nil {
		defer taskProcessor.Close()
		taskProcessor.SetLogger(jobLogger)
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

	// Step 3: Upload the original file or processed one
	err = uploadUpstream(w, r, uploadFile, uploadFilename)
	if err != nil {
		jobLogger.Printf("upload upstream error: %s", err.Error())
		http.Error(w, "failed to process file, view IUO logs for more info", http.StatusInternalServerError)
	}
	if uploadOriginal {
		jobLogger.Printf("uploaded original: \"%s\" (%s)", formFileHeader.Filename, humanReadableSize(formFileHeader.Size))
	} else {
		if newHash, err := SHA1(taskProcessor.ProcessedFile); err != nil {
			return fmt.Errorf("new sha1: %w", err)
		} else {
			addChecksums(newHash, originalHash)
		}
		jobLogger.Printf("uploaded: \"%s\" (%s) <- (%s) \"%s\"", taskProcessor.ProcessedFilename, humanReadableSize(taskProcessor.ProcessedSize), humanReadableSize(taskProcessor.OriginalSize), taskProcessor.OriginalFilename)
	}

	return nil
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
