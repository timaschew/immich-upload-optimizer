package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
)

// Android motion photos are a JPEG with an MP4 appended after the image data. The JPEG header describes the
// layout in its XMP packet, either with Google's container directory (newer devices) or with the legacy
// MicroVideo offset. Converting such a file with avifenc/ImageMagick silently drops the video, while the XMP
// keeps claiming it is there, which makes the immich server read garbage at the offset it computes from the
// directory (server/src/services/metadata.service.ts, applyMotionPhotos).
//
// Splitting the file before the task runs gives the still image to the configured image task, the video to the
// configured video task, and lets both be uploaded as two assets linked through livePhotoVideoId, the same way
// the mobile app uploads an iOS live photo.
//
// Samsung HEIC motion photos are not covered: they store the video as an exif binary tag instead of appending
// it, so there is no offset to cut at.

// Only the JPEG header is searched for XMP, which sits well before the image data (~34 KB into a Pixel photo).
const maxXMPScan = 1 << 20

// jpegSOI is the start of image marker every JPEG begins with.
var jpegSOI = []byte{0xFF, 0xD8}

var (
	xmpStandardID = []byte("http://ns.adobe.com/xap/1.0/\x00")
	xmpExtendedID = []byte("http://ns.adobe.com/xmp/extension/\x00")

	// Matches GCamera:MotionPhoto="1" and GCamera:MicroVideo="1", plus their element form. The tag name is
	// followed by a separator in both patterns so MotionPhotoVersion and MotionPhotoPresentationTimestampUs
	// can't match.
	motionFlagAttr    = regexp.MustCompile(`GCamera:(?:MotionPhoto|MicroVideo)\s*=\s*["'](\d)["']`)
	motionFlagElement = regexp.MustCompile(`<GCamera:(?:MotionPhoto|MicroVideo)>\s*(\d)\s*<`)

	microVideoOffsetRe = regexp.MustCompile(`GCamera:MicroVideoOffset\s*=\s*["'](\d+)["']`)
	// Element start tags carrying container item attributes. Attribute values cannot contain angle brackets,
	// so this matches the whole tag regardless of whether the writer used <Container:Item .../> or put the
	// attributes on <rdf:li .../>.
	containerItemRe = regexp.MustCompile(`<[^<>]*Item:Semantic[^<>]*>`)
	itemSemanticRe  = regexp.MustCompile(`Item:Semantic\s*=\s*["']([^"']*)["']`)
	itemLengthRe    = regexp.MustCompile(`Item:Length\s*=\s*["'](\d+)["']`)
	itemPaddingRe   = regexp.MustCompile(`Item:Padding\s*=\s*["'](\d+)["']`)
)

// motionPhoto describes what was found in the XMP of an upload.
type motionPhoto struct {
	// VideoOffset is where the embedded MP4 starts, 0 when the file claims to be a motion photo but no video
	// could be located. VideoLength is the number of bytes from there to the end of the file.
	VideoOffset int64
	VideoLength int64
	// DeclaredLength is how many trailing bytes the XMP says the video occupies, even when nothing usable was
	// found there. It is what immich would subtract from the file size to locate the video, so a file with a
	// declared length is one immich reads at a wrong position once the video is gone.
	DeclaredLength int64
	// FlagOffsets are the file offsets of the digit in every motion photo flag that is set. Overwriting them
	// with '0' keeps the XMP byte length intact and stops immich from looking for a video that is gone.
	FlagOffsets []int64
}

// xmpPacket is one XMP payload together with its absolute position in the file.
type xmpPacket struct {
	offset int64
	data   []byte
}

// detectMotionPhoto reports whether f is an Android motion photo. It returns nil for everything else,
// including plain JPEGs that merely carry a container directory (a Pixel writes one for the HDR gain map).
func detectMotionPhoto(f *os.File, size int64) (*motionPhoto, error) {
	header := make([]byte, 2)
	if _, err := f.ReadAt(header, 0); err != nil {
		return nil, nil
	}
	if !bytes.Equal(header, jpegSOI) {
		return nil, nil
	}
	packets, sosOffset, err := readXMPPackets(f, size)
	if err != nil || len(packets) == 0 {
		return nil, err
	}

	mp := &motionPhoto{}
	for _, packet := range packets {
		for _, re := range []*regexp.Regexp{motionFlagAttr, motionFlagElement} {
			for _, match := range re.FindAllSubmatchIndex(packet.data, -1) {
				if packet.data[match[2]] == '0' {
					continue
				}
				mp.FlagOffsets = append(mp.FlagOffsets, packet.offset+int64(match[2]))
			}
		}
	}
	if len(mp.FlagOffsets) == 0 {
		return nil, nil
	}

	for _, packet := range packets {
		trailers := []int64{containerTrailer(packet.data)}
		if match := microVideoOffsetRe.FindSubmatch(packet.data); match != nil {
			trailer, convErr := strconv.ParseInt(string(match[1]), 10, 64)
			if convErr == nil {
				trailers = append(trailers, trailer)
			}
		}
		for _, trailer := range trailers {
			if trailer <= 0 || trailer >= size {
				continue
			}
			if mp.DeclaredLength == 0 {
				mp.DeclaredLength = trailer
			}
			if offset := size - trailer; validMP4At(f, size, offset) {
				mp.VideoOffset = offset
				break
			}
		}
		if mp.VideoOffset > 0 {
			break
		}
	}
	if mp.VideoOffset == 0 {
		mp.VideoOffset = scanForMP4(f, size, sosOffset)
	}
	if mp.VideoOffset > 0 {
		mp.VideoLength = size - mp.VideoOffset
	}
	return mp, nil
}

// readXMPPackets walks the JPEG marker segments up to the start of the image data and returns every APP1 XMP
// payload, standard and extended, along with the offset the image data starts at.
func readXMPPackets(f *os.File, size int64) (packets []xmpPacket, sosOffset int64, err error) {
	marker := make([]byte, 4)
	pos := int64(2)
	for pos+4 <= size && pos < maxXMPScan {
		if _, err = f.ReadAt(marker, pos); err != nil {
			return packets, pos, nil
		}
		if marker[0] != 0xFF {
			return packets, pos, nil
		}
		switch code := marker[1]; {
		case code == 0xD8 || code == 0x01 || (code >= 0xD0 && code <= 0xD7):
			// Standalone markers without a payload.
			pos += 2
			continue
		case code == 0xDA || code == 0xD9:
			// Image data or end of image, no more metadata beyond this point.
			return packets, pos, nil
		}
		segLen := int64(binary.BigEndian.Uint16(marker[2:4]))
		if segLen < 2 || pos+2+segLen > size {
			return packets, pos, nil
		}
		if marker[1] == 0xE1 {
			payload := make([]byte, segLen-2)
			if _, err = f.ReadAt(payload, pos+4); err != nil {
				return packets, pos, nil
			}
			for _, id := range [][]byte{xmpStandardID, xmpExtendedID} {
				if bytes.HasPrefix(payload, id) {
					packets = append(packets, xmpPacket{offset: pos + 4 + int64(len(id)), data: payload[len(id):]})
					break
				}
			}
		}
		pos += 2 + segLen
	}
	return packets, pos, nil
}

// containerTrailer resolves Google's container directory to the number of bytes the MotionPhoto item and
// everything after it occupy. Items are stored in document order directly after the primary image, so that is
// also how far before the end of the file the video starts.
func containerTrailer(xmp []byte) int64 {
	items := containerItemRe.FindAll(xmp, -1)
	trailer := int64(0)
	found := false
	for i := len(items) - 1; i >= 0; i-- {
		length := int64(0)
		if match := itemLengthRe.FindSubmatch(items[i]); match != nil {
			length, _ = strconv.ParseInt(string(match[1]), 10, 64)
		}
		padding := int64(0)
		if match := itemPaddingRe.FindSubmatch(items[i]); match != nil {
			padding, _ = strconv.ParseInt(string(match[1]), 10, 64)
		}
		trailer += length + padding
		if match := itemSemanticRe.FindSubmatch(items[i]); match != nil && string(match[1]) == "MotionPhoto" {
			found = true
			break
		}
	}
	if !found {
		return 0
	}
	return trailer
}

// scanForMP4 looks for an MP4 that reaches the end of the file, for motion photos whose XMP announces a video
// without saying where it is. Only called once a motion photo flag was seen, so a plain JPEG that happens to
// contain the bytes "ftyp" is never scanned.
func scanForMP4(f *os.File, size, from int64) int64 {
	const window = 1 << 20
	needle := []byte("ftyp")
	buf := make([]byte, window)
	for pos := from; pos < size; pos += window - int64(len(needle)) {
		n, err := f.ReadAt(buf, pos)
		if n <= 0 {
			if err != nil {
				return 0
			}
			continue
		}
		chunk := buf[:n]
		for searched := 0; ; {
			i := bytes.Index(chunk[searched:], needle)
			if i < 0 {
				break
			}
			searched += i + 1
			// The box size precedes the box type.
			if candidate := pos + int64(searched-1) - 4; candidate > 0 && validMP4At(f, size, candidate) {
				return candidate
			}
		}
		if err != nil {
			return 0
		}
	}
	return 0
}

// validMP4At reports whether an MP4 starts at off and its box chain adds up to exactly the end of the file.
// Requiring the chain to end at EOF is what keeps the offset trustworthy enough to cut the file at.
func validMP4At(f *os.File, size, off int64) bool {
	if off <= 0 || off >= size || size-off < 16 {
		return false
	}
	header := make([]byte, 16)
	pos := off
	for boxes := 0; pos < size; boxes++ {
		if boxes > 1024 {
			return false
		}
		n, err := f.ReadAt(header, pos)
		if n < 8 && err != nil {
			return false
		}
		boxSize := int64(binary.BigEndian.Uint32(header[0:4]))
		boxType := header[4:8]
		if pos == off && !bytes.Equal(boxType, []byte("ftyp")) {
			return false
		}
		for _, c := range boxType {
			if c < 0x20 || c > 0x7E {
				return false
			}
		}
		switch {
		case boxSize == 0:
			// A box of size 0 runs to the end of the file.
			return true
		case boxSize == 1:
			if n < 16 {
				return false
			}
			boxSize = int64(binary.BigEndian.Uint64(header[8:16]))
			if boxSize < 16 {
				return false
			}
		case boxSize < 8:
			return false
		}
		pos += boxSize
	}
	return pos == size
}

// patchMotionFlags clears every motion photo flag in place. The digit is overwritten, so the XMP packet keeps
// its length and the APP1 segment stays valid, and exiftool reports MotionPhoto as 0 which immich treats as
// "not a motion photo" (metadata.service.ts, isMotionPhoto).
func patchMotionFlags(f *os.File, mp *motionPhoto) error {
	for _, offset := range mp.FlagOffsets {
		if _, err := f.WriteAt([]byte{'0'}, offset); err != nil {
			return fmt.Errorf("unable to clear motion photo flag at %d: %w", offset, err)
		}
	}
	return nil
}

// splitMotionPhoto writes the embedded video to its own temp file and turns f into the still image by clearing
// the motion photo flags and truncating it. f is reused instead of copied because the temp directory is
// typically a tmpfs, so only the video needs additional space.
func splitMotionPhoto(f *os.File, size int64, mp *motionPhoto) (video *os.File, stillSize int64, err error) {
	if video, err = os.CreateTemp("", "motion-*.mp4"); err != nil {
		return nil, 0, fmt.Errorf("unable to create temp file: %w", err)
	}
	discard := func() {
		video.Close()
		_ = os.Remove(video.Name())
	}
	if _, err = io.Copy(video, io.NewSectionReader(f, mp.VideoOffset, mp.VideoLength)); err != nil {
		discard()
		return nil, 0, fmt.Errorf("unable to extract embedded video: %w", err)
	}
	if err = patchMotionFlags(f, mp); err != nil {
		discard()
		return nil, 0, err
	}
	if err = f.Truncate(mp.VideoOffset); err != nil {
		discard()
		return nil, 0, fmt.Errorf("unable to truncate still image: %w", err)
	}
	return video, mp.VideoOffset, nil
}
