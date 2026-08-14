package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// receivedUpload is what the upstream stub saw of one /api/assets request.
type receivedUpload struct {
	fields   map[string]string
	filename string
	size     int64
}

// stubUpstream stands in for the immich server and answers uploads with a new asset id.
func stubUpstream(t *testing.T, uploads *[]receivedUpload) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/assets" || r.Method != "POST" {
			t.Errorf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			t.Errorf("parse upstream form: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		upload := receivedUpload{fields: map[string]string{}}
		for key, values := range r.MultipartForm.Value {
			upload.fields[key] = values[0]
		}
		if files := r.MultipartForm.File["assetData"]; len(files) == 1 {
			upload.filename = files[0].Filename
			upload.size = files[0].Size
		} else {
			t.Errorf("expected exactly one assetData part, got %d", len(files))
		}
		*uploads = append(*uploads, upload)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(assetMediaResponse{ID: "asset-" + string(rune('a'+len(*uploads))), Status: "created"})
	}))
	t.Cleanup(server.Close)

	upstreamURL = server.URL
	var err error
	if remote, err = url.Parse(server.URL); err != nil {
		t.Fatalf("parse stub url: %v", err)
	}
}

// prepareJobEnv sets the globals main would have initialised, without any task configured so no external
// converter is needed.
func prepareJobEnv(t *testing.T) {
	t.Helper()
	DevMITMproxy = false
	motionPhotoSplit = true
	config = &Config{}
	imageSemaphore = make(chan struct{}, 1)
	videoSemaphore = make(chan struct{}, 1)
	baseLogger = log.New(io.Discard, "", 0)
	checksumsFile = filepath.Join(t.TempDir(), "checksums.csv")
	mapLock.Lock()
	fakeToOriginalChecksum = map[string]string{}
	originalToFakeChecksum = map[string]string{}
	mapLock.Unlock()
}

// uploadRequest builds the multipart request the immich mobile app sends for one asset.
func uploadRequest(t *testing.T, path string) *http.Request {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Skipf("sample not available: %v", err)
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for key, value := range map[string]string{
		"deviceAssetId":  "test-motion-photo",
		"deviceId":       "test-device",
		"fileCreatedAt":  "2026-08-07T07:39:05.000Z",
		"fileModifiedAt": "2026-08-07T07:39:05.000Z",
		"isFavorite":     "false",
		"duration":       "0",
		"filename":       filepath.Base(path),
	} {
		if err = writer.WriteField(key, value); err != nil {
			t.Fatalf("write field %s: %v", key, err)
		}
	}
	part, err := writer.CreateFormFile("assetData", filepath.Base(path))
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err = io.Copy(part, file); err != nil {
		t.Fatalf("copy sample: %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	request := httptest.NewRequest("POST", "/api/assets", body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("x-api-key", "test-key")
	return request
}

func TestNewJobSplitsMotionPhoto(t *testing.T) {
	prepareJobEnv(t)
	var uploads []receivedUpload
	stubUpstream(t, &uploads)

	request := uploadRequest(t, motionSample)
	recorder := httptest.NewRecorder()
	if err := newJob(request, recorder, newCustomLogger(baseLogger, "")); err != nil {
		t.Fatalf("newJob: %v", err)
	}

	if len(uploads) != 2 {
		t.Fatalf("got %d uploads, want 2 (video then still)", len(uploads))
	}
	video, still := uploads[0], uploads[1]

	// The video has to go first, its id is what links the two assets.
	if video.filename != "PXL_20260807_073905617.MP.mp4" {
		t.Errorf("video filename = %q", video.filename)
	}
	if video.fields["filename"] != "PXL_20260807_073905617.MP.mp4" {
		t.Errorf("video filename field = %q", video.fields["filename"])
	}
	if video.size != motionVideoLength {
		t.Errorf("video size = %d, want %d", video.size, motionVideoLength)
	}
	if video.fields["visibility"] != "hidden" {
		t.Errorf("video visibility = %q, want hidden", video.fields["visibility"])
	}
	if _, linked := video.fields["livePhotoVideoId"]; linked {
		t.Error("video must not carry a livePhotoVideoId itself")
	}
	if video.fields["deviceAssetId"] != "test-motion-photo" {
		t.Errorf("video deviceAssetId = %q", video.fields["deviceAssetId"])
	}

	if still.filename != "PXL_20260807_073905617.MP.jpg" {
		t.Errorf("still filename = %q", still.filename)
	}
	if still.size != motionVideoOffset {
		t.Errorf("still size = %d, want %d", still.size, motionVideoOffset)
	}
	if still.fields["livePhotoVideoId"] != "asset-b" {
		t.Errorf("still livePhotoVideoId = %q, want the id the stub gave the video", still.fields["livePhotoVideoId"])
	}
	if _, hidden := still.fields["visibility"]; hidden {
		t.Error("still must not be uploaded hidden")
	}

	if recorder.Code != http.StatusCreated {
		t.Errorf("client got status %d, want %d", recorder.Code, http.StatusCreated)
	}
	var answer assetMediaResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &answer); err != nil || answer.ID != "asset-c" {
		t.Errorf("client got %q, want the id of the still image", recorder.Body.String())
	}

	// Without a mapping from the checksum of the whole .MP.jpg to the still image, the client would upload the
	// photo again on the next sync.
	wantOriginal := sampleChecksum(t, motionSample, 0)
	wantStill := splitStillChecksum(t, motionSample)
	waitForChecksum(t, wantOriginal, wantStill)
}

func TestNewJobLeavesPlainPhotoAlone(t *testing.T) {
	prepareJobEnv(t)
	var uploads []receivedUpload
	stubUpstream(t, &uploads)

	request := uploadRequest(t, plainSample)
	recorder := httptest.NewRecorder()
	if err := newJob(request, recorder, newCustomLogger(baseLogger, "")); err != nil {
		t.Fatalf("newJob: %v", err)
	}

	if len(uploads) != 1 {
		t.Fatalf("got %d uploads, want 1", len(uploads))
	}
	if _, linked := uploads[0].fields["livePhotoVideoId"]; linked {
		t.Error("plain photo was linked to a video")
	}
	if uploads[0].filename != filepath.Base(plainSample) {
		t.Errorf("filename = %q", uploads[0].filename)
	}
	mapLock.RLock()
	defer mapLock.RUnlock()
	if len(originalToFakeChecksum) != 0 {
		t.Errorf("unconverted upload registered a checksum mapping: %v", originalToFakeChecksum)
	}
}

// A video that was uploaded but whose still image immich rejects would stay behind as an asset nobody can see
// in the timeline, so it has to be removed again.
func TestNewJobRemovesVideoWhenStillFails(t *testing.T) {
	prepareJobEnv(t)
	var deleted []string
	uploads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			uploads++
			if uploads == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(assetMediaResponse{ID: "video-1", Status: "created"})
				return
			}
			http.Error(w, `{"message":"Not found or no asset.upload access"}`, http.StatusBadRequest)
		case "DELETE":
			var body struct {
				IDs   []string `json:"ids"`
				Force bool     `json:"force"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode delete body: %v", err)
			}
			if !body.Force {
				t.Error("delete request without force")
			}
			deleted = append(deleted, body.IDs...)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)
	upstreamURL = server.URL

	recorder := httptest.NewRecorder()
	err := newJob(uploadRequest(t, motionSample), recorder, newCustomLogger(baseLogger, ""))
	if err == nil {
		t.Error("newJob reported success although immich rejected the still image")
	}
	if len(deleted) != 1 || deleted[0] != "video-1" {
		t.Errorf("deleted assets = %v, want [video-1]", deleted)
	}
	if recorder.Code != http.StatusBadRequest {
		t.Errorf("client got status %d, want the %d immich answered with", recorder.Code, http.StatusBadRequest)
	}
	mapLock.RLock()
	defer mapLock.RUnlock()
	if len(originalToFakeChecksum) != 0 {
		t.Errorf("a rejected upload registered a checksum mapping: %v", originalToFakeChecksum)
	}
}

// sampleChecksum hashes the first limit bytes of a sample, or all of it when limit is 0.
func sampleChecksum(t *testing.T, name string, limit int64) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Skipf("sample not available: %v", err)
	}
	if limit > 0 {
		data = data[:limit]
	}
	sum := sha1.Sum(data)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// splitStillChecksum is the checksum of what the still image looks like after the split: cut at the video
// offset with the motion photo flag cleared.
func splitStillChecksum(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Skipf("sample not available: %v", err)
	}
	still := append([]byte(nil), data[:motionVideoOffset]...)
	still[motionFlagOffset] = '0'
	sum := sha1.Sum(still)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// waitForChecksum polls the mapping, which addChecksums fills in from a goroutine.
func waitForChecksum(t *testing.T, original, fake string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mapLock.RLock()
		got, ok := originalToFakeChecksum[original]
		mapLock.RUnlock()
		if ok && got == fake {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("checksum mapping for the original is %q, want %q", got, fake)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
