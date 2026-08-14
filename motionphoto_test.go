package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Sample files from a Pixel 9a. They are not part of the repository because of their size, the tests covering
// them skip when they are missing.
const (
	motionSample = "android-live/PXL_20260807_073905617.MP.jpg"
	plainSample  = "android-live/PXL_20260811_190701434.jpg"

	// Layout of motionSample: 3889188 bytes primary JPEG + 41809 bytes HDR gain map + 1925160 bytes MP4.
	motionSampleSize  = 5856157
	motionVideoOffset = 3930997
	motionVideoLength = 1925160
	// The digit of GCamera:MotionPhoto="1", the attribute itself starts at 34410.
	motionFlagOffset = 34431
)

func openSample(t *testing.T, name string) (*os.File, int64) {
	t.Helper()
	file, err := os.Open(name)
	if err != nil {
		t.Skipf("sample not available: %v", err)
	}
	t.Cleanup(func() { file.Close() })
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat %s: %v", name, err)
	}
	return file, info.Size()
}

// copySample returns a writable copy, so the test never modifies the sample itself.
func copySample(t *testing.T, name string) (*os.File, int64) {
	t.Helper()
	source, size := openSample(t, name)
	target, err := os.Create(filepath.Join(t.TempDir(), filepath.Base(name)))
	if err != nil {
		t.Fatalf("create copy: %v", err)
	}
	t.Cleanup(func() { target.Close() })
	if _, err = io.Copy(target, source); err != nil {
		t.Fatalf("copy sample: %v", err)
	}
	return target, size
}

func TestDetectMotionPhoto(t *testing.T) {
	file, size := openSample(t, motionSample)
	if size != motionSampleSize {
		t.Fatalf("sample changed: size %d, want %d", size, motionSampleSize)
	}
	mp, err := detectMotionPhoto(file, size)
	if err != nil {
		t.Fatalf("detectMotionPhoto: %v", err)
	}
	if mp == nil {
		t.Fatal("motion photo not detected")
	}
	if mp.VideoOffset != motionVideoOffset {
		t.Errorf("VideoOffset = %d, want %d", mp.VideoOffset, motionVideoOffset)
	}
	if mp.VideoLength != motionVideoLength {
		t.Errorf("VideoLength = %d, want %d", mp.VideoLength, motionVideoLength)
	}
	if mp.DeclaredLength != motionVideoLength {
		t.Errorf("DeclaredLength = %d, want %d", mp.DeclaredLength, motionVideoLength)
	}
	if len(mp.FlagOffsets) != 1 || mp.FlagOffsets[0] != motionFlagOffset {
		t.Errorf("FlagOffsets = %v, want [%d]", mp.FlagOffsets, motionFlagOffset)
	}
}

// A regular Pixel photo carries a container directory too, for its HDR gain map, and its last EOI marker is at
// the very end of the file. Neither may be mistaken for a motion photo.
func TestDetectPlainPhoto(t *testing.T) {
	file, size := openSample(t, plainSample)
	mp, err := detectMotionPhoto(file, size)
	if err != nil {
		t.Fatalf("detectMotionPhoto: %v", err)
	}
	if mp != nil {
		t.Errorf("plain photo detected as motion photo: %+v", mp)
	}
}

// A motion photo whose video was already stripped, which is what a converted file looks like: the XMP still
// announces the video, so immich would read at an offset that no longer holds one. The flag has to be
// recognised as clearable even though there is nothing left to split off.
func TestDetectStrippedMotionPhoto(t *testing.T) {
	file, _ := copySample(t, motionSample)
	if err := file.Truncate(motionVideoOffset); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	mp, err := detectMotionPhoto(file, motionVideoOffset)
	if err != nil {
		t.Fatalf("detectMotionPhoto: %v", err)
	}
	if mp == nil {
		t.Fatal("stripped motion photo not recognised")
	}
	if mp.VideoOffset != 0 {
		t.Errorf("VideoOffset = %d, want 0", mp.VideoOffset)
	}
	if mp.DeclaredLength != motionVideoLength {
		t.Errorf("DeclaredLength = %d, want %d", mp.DeclaredLength, motionVideoLength)
	}
	if len(mp.FlagOffsets) != 1 {
		t.Errorf("FlagOffsets = %v, want one offset to clear", mp.FlagOffsets)
	}
}

// A motion photo flag without any offset information, the shape Samsung writes: the server may still be able to
// extract the video from an exif binary tag, so nothing about the file may be touched.
func TestDetectMotionPhotoWithoutLocation(t *testing.T) {
	file, size := syntheticJPEG(t, `<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">`+
		`<rdf:Description rdf:about="" xmlns:GCamera="http://ns.google.com/photos/1.0/camera/" GCamera:MotionPhoto="1"/>`+
		`</rdf:RDF></x:xmpmeta>`)
	mp, err := detectMotionPhoto(file, size)
	if err != nil {
		t.Fatalf("detectMotionPhoto: %v", err)
	}
	if mp == nil {
		t.Fatal("motion photo flag not found")
	}
	if mp.VideoOffset != 0 || mp.DeclaredLength != 0 {
		t.Errorf("VideoOffset = %d, DeclaredLength = %d, want 0, 0", mp.VideoOffset, mp.DeclaredLength)
	}
	if len(mp.FlagOffsets) != 1 {
		t.Fatalf("FlagOffsets = %v, want one offset", mp.FlagOffsets)
	}
	flag := make([]byte, 1)
	if _, err = file.ReadAt(flag, mp.FlagOffsets[0]); err != nil || flag[0] != '1' {
		t.Errorf("flag offset points at %q, %v; want '1'", flag, err)
	}
}

// A flag that is already 0 is not a motion photo and must not be reported as one.
func TestDetectClearedFlag(t *testing.T) {
	file, size := syntheticJPEG(t, `<rdf:Description xmlns:GCamera="http://ns.google.com/photos/1.0/camera/" GCamera:MotionPhoto="0"/>`)
	mp, err := detectMotionPhoto(file, size)
	if err != nil || mp != nil {
		t.Errorf("detectMotionPhoto = %+v, %v; want nil, nil", mp, err)
	}
}

// syntheticJPEG writes a minimal JPEG whose only content is an APP1 segment with the given XMP packet.
func syntheticJPEG(t *testing.T, xmp string) (*os.File, int64) {
	t.Helper()
	payload := make([]byte, 0, len(xmpStandardID)+len(xmp))
	payload = append(payload, xmpStandardID...)
	payload = append(payload, xmp...)

	data := []byte{0xFF, 0xD8, 0xFF, 0xE1}
	data = binary.BigEndian.AppendUint16(data, uint16(len(payload)+2))
	data = append(data, payload...)
	data = append(data, 0xFF, 0xD9)

	name := filepath.Join(t.TempDir(), "synthetic.jpg")
	if err := os.WriteFile(name, data, 0o600); err != nil {
		t.Fatalf("write synthetic jpeg: %v", err)
	}
	file, err := os.Open(name)
	if err != nil {
		t.Fatalf("open synthetic jpeg: %v", err)
	}
	t.Cleanup(func() { file.Close() })
	return file, int64(len(data))
}

func TestDetectNonJPEG(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "video-*.mp4")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	defer file.Close()
	if _, err = file.WriteString("\x00\x00\x00\x18ftypmp42GCamera:MotionPhoto=\"1\""); err != nil {
		t.Fatalf("write: %v", err)
	}
	mp, err := detectMotionPhoto(file, 42)
	if err != nil || mp != nil {
		t.Errorf("detectMotionPhoto = %+v, %v; want nil, nil", mp, err)
	}
}

func TestSplitMotionPhoto(t *testing.T) {
	file, size := copySample(t, motionSample)
	mp, err := detectMotionPhoto(file, size)
	if err != nil || mp == nil {
		t.Fatalf("detectMotionPhoto = %+v, %v", mp, err)
	}
	video, stillSize, err := splitMotionPhoto(file, size, mp)
	if err != nil {
		t.Fatalf("splitMotionPhoto: %v", err)
	}
	defer func() { video.Close(); os.Remove(video.Name()) }()

	if stillSize != motionVideoOffset {
		t.Errorf("still size = %d, want %d", stillSize, motionVideoOffset)
	}
	stillInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("stat still: %v", err)
	}
	if stillInfo.Size() != motionVideoOffset {
		t.Errorf("still file size = %d, want %d", stillInfo.Size(), motionVideoOffset)
	}
	head := make([]byte, 2)
	if _, err = file.ReadAt(head, 0); err != nil || !bytes.Equal(head, jpegSOI) {
		t.Errorf("still does not start with a JPEG SOI marker: %x, %v", head, err)
	}

	videoInfo, err := video.Stat()
	if err != nil {
		t.Fatalf("stat video: %v", err)
	}
	if videoInfo.Size() != motionVideoLength {
		t.Errorf("video size = %d, want %d", videoInfo.Size(), motionVideoLength)
	}
	boxType := make([]byte, 4)
	if _, err = video.ReadAt(boxType, 4); err != nil || !bytes.Equal(boxType, []byte("ftyp")) {
		t.Errorf("video does not start with an ftyp box: %q, %v", boxType, err)
	}

	// The flag has to be cleared, otherwise immich keeps looking for a video that is no longer there.
	flag := make([]byte, 1)
	if _, err = file.ReadAt(flag, motionFlagOffset); err != nil || flag[0] != '0' {
		t.Errorf("motion photo flag = %q, %v; want '0'", flag, err)
	}
	// Which the detection itself is the best check for: the still is no longer a motion photo.
	again, err := detectMotionPhoto(file, stillSize)
	if err != nil {
		t.Fatalf("detectMotionPhoto after split: %v", err)
	}
	if again != nil {
		t.Errorf("split still detected as motion photo: %+v", again)
	}
}
