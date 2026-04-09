package main

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

var jobIdCounter atomic.Int64
var jobs sync.Map // map[string]int

func newJob(r *http.Request, w http.ResponseWriter, logger *customLogger) error {
	jobID := jobIdCounter.Add(1)
	jobLogger := newCustomLogger(logger, fmt.Sprintf("job %d: ", jobID))

	// Use streaming multipart reader to read form fields before the file. This allows checking for duplicate jobs before downloading the full asset data.
	mr, err := r.MultipartReader()
	if err != nil {
		return fmt.Errorf("unable to create multipart reader: %w", err)
	}

	formValues := make(map[string][]string)
	var filePart *multipart.Part
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("unable to read multipart part: %w", err)
		}
		if part.FileName() != "" {
			filePart = part
			break
		}
		fieldName := part.FormName()
		value, err := io.ReadAll(part)
		part.Close()
		if err != nil {
			return fmt.Errorf("unable to read form field %s: %w", fieldName, err)
		}
		formValues[fieldName] = append(formValues[fieldName], string(value))
	}
	if filePart == nil {
		return fmt.Errorf("no file found in multipart form data")
	}
	defer filePart.Close()

	// Check for duplicate job using deviceAssetId before downloading the file
	jobKey := ""
	if ids, ok := formValues["deviceAssetId"]; ok && len(ids) > 0 {
		jobKey = ids[0]
	}
	if jobKey == "" {
		// Never happens, but just in case
		jobKey = filePart.FileName()
	}

	jobLogger.Printf("received: \"%s\" (deviceAssetId: %s)", filePart.FileName(), jobKey)
	if existingID, loaded := jobs.LoadOrStore(jobKey, jobID); loaded {
		// Hijack the connection to immediately stop the app from sending more data for this asset (currently the iOS app http client is a bit buggy and stops uploading other unrelated assets too while the Android app only stops this upload)
		if hj, ok := w.(http.Hijacker); ok {
			conn, bufrw, err := hj.Hijack()
			if err == nil {
				_, _ = bufrw.WriteString("HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nIUO is already processing this asset.\r\n")
				_ = bufrw.Flush()
				conn.Close()
			}
		}
		return fmt.Errorf("a job processing this file already exists with ID: %d", existingID)
	}
	defer jobs.Delete(jobKey)

	// Download original file
	fileName := filePart.FileName()
	tmpFile, err := os.CreateTemp("", "upload-*"+path.Ext(fileName))
	if err != nil {
		return fmt.Errorf("unable to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	fileSize, err := io.Copy(tmpFile, filePart)
	if err != nil {
		return fmt.Errorf("unable to save uploaded file: %w", err)
	}
	filePart.Close()
	jobLogger.Printf("download original: \"%s\" (%s)", fileName, humanReadableSize(fileSize))
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

	if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("unable to seek temp file: %w", err)
	}

	var originalHash string
	var newHash string
	uploadFile := io.ReadSeeker(tmpFile)
	uploadFilename := fileName
	uploadOriginal := true

	taskProcessor, err := NewTaskProcessor(tmpFile, fileName, fileSize)
	if err == nil && taskProcessor != nil {
		defer taskProcessor.Close()
		taskProcessor.SetLogger(jobLogger)
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
	err = uploadUpstream(w, r, uploadFile, uploadFilename, formValues)
	if err != nil {
		jobLogger.Printf("upload upstream error: %s", err.Error())
		http.Error(w, "failed to process file, view IUO logs for more info", http.StatusInternalServerError)
		return err
	}
	if uploadOriginal {
		jobLogger.Printf("uploaded original: \"%s\" (%s)", fileName, humanReadableSize(fileSize))
	} else {
		if newHash, err = SHA1(taskProcessor.ProcessedFile); err != nil {
			return fmt.Errorf("new sha1: %w", err)
		}
		addChecksums(newHash, originalHash)
		jobLogger.Printf("uploaded: \"%s\" (%s) <- (%s) \"%s\"", taskProcessor.ProcessedFilename, humanReadableSize(taskProcessor.ProcessedSize), humanReadableSize(taskProcessor.OriginalSize), taskProcessor.OriginalFilename)
	}

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
