package treasuredata

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// createTarGz creates a tar.gz archive from a directory
func createTarGz(sourceDir string) ([]byte, error) {
	// Define reasonable limits
	const (
		maxFileSize  = 100 * 1024 * 1024 // 100MB per file
		maxTotalSize = 500 * 1024 * 1024 // 500MB total archive size
		maxFiles     = 10000             // Maximum number of files
	)

	var (
		buf       bytes.Buffer
		totalSize int64
		fileCount int
	)

	// Ensure sourceDir is absolute
	absSourceDir, err := filepath.Abs(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	err = filepath.Walk(absSourceDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Security check: reject symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinks not allowed: %s", filePath)
		}

		// Get relative path
		relPath, err := filepath.Rel(absSourceDir, filePath)
		if err != nil {
			return err
		}

		// Security check: ensure path doesn't escape source directory
		if strings.HasPrefix(relPath, "..") || filepath.IsAbs(relPath) {
			return fmt.Errorf("path traversal detected: %s", relPath)
		}

		// Skip hidden files and directories (starting with .)
		if strings.HasPrefix(filepath.Base(filePath), ".") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Check file count limit
		fileCount++
		if fileCount > maxFiles {
			return fmt.Errorf("too many files: maximum %d files allowed", maxFiles)
		}

		// Check file size limit
		if info.Mode().IsRegular() && info.Size() > maxFileSize {
			return fmt.Errorf("file too large: %s (size: %d bytes, max: %d bytes)", filePath, info.Size(), maxFileSize)
		}

		// Check total size limit
		totalSize += info.Size()
		if totalSize > maxTotalSize {
			return fmt.Errorf("archive too large: total size %d bytes exceeds maximum %d bytes", totalSize, maxTotalSize)
		}

		// Create tar header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		// Use relative path as name
		header.Name = relPath

		// Write header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// If it's a regular file, write the content
		if info.Mode().IsRegular() {
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}

			// Ensure file is closed even if io.Copy fails
			_, copyErr := io.Copy(tw, file)
			closeErr := file.Close()

			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Close tar writer
	if err := tw.Close(); err != nil {
		return nil, err
	}

	// Close gzip writer
	if err := gw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// extractTarGz extracts a tar.gz archive to a directory with security validations
func extractTarGz(archiveData []byte, outputDir string) error {
	// Define reasonable limits for extraction
	const (
		maxFileSize  = 100 * 1024 * 1024 // 100MB per file
		maxTotalSize = 500 * 1024 * 1024 // 500MB total extracted size
		maxFiles     = 10000             // Maximum number of files
	)

	var (
		totalSize int64
		fileCount int
	)

	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get absolute path of output directory for security checks
	absOutputDir, err := filepath.Abs(outputDir)
	if err != nil {
		return fmt.Errorf("failed to get absolute output directory path: %w", err)
	}
	absOutputDir = filepath.Clean(absOutputDir)

	// Create gzip reader
	gzipReader, err := gzip.NewReader(bytes.NewReader(archiveData))
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	// Create tar reader
	tarReader := tar.NewReader(gzipReader)

	// Extract each file from the archive
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Security check: validate file path
		if header.Name == "" {
			continue // Skip empty names
		}

		// Clean the file path and check for path traversal
		cleanPath := filepath.Clean(header.Name)
		if strings.Contains(cleanPath, "..") || filepath.IsAbs(cleanPath) {
			return fmt.Errorf("unsafe file path in archive: %s", header.Name)
		}

		// Create full output path
		outputPath := filepath.Join(absOutputDir, cleanPath)

		// Security check: ensure the output path is within the output directory
		if !strings.HasPrefix(outputPath, absOutputDir+string(filepath.Separator)) && outputPath != absOutputDir {
			return fmt.Errorf("path traversal detected: %s", header.Name)
		}

		// Check file count limit
		fileCount++
		if fileCount > maxFiles {
			return fmt.Errorf("too many files in archive: maximum %d files allowed", maxFiles)
		}

		// Check file size limit
		if header.Size > maxFileSize {
			return fmt.Errorf("file too large in archive: %s (size: %d bytes, max: %d bytes)",
				header.Name, header.Size, maxFileSize)
		}

		// Check total size limit
		totalSize += header.Size
		if totalSize > maxTotalSize {
			return fmt.Errorf("archive too large: total size %d bytes exceeds maximum %d bytes",
				totalSize, maxTotalSize)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory
			if err := os.MkdirAll(outputPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", outputPath, err)
			}

		case tar.TypeReg:
			// Create parent directory if it doesn't exist
			if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory for %s: %w", outputPath, err)
			}

			// Create the file
			file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", outputPath, err)
			}

			// Copy file content with size limit
			_, err = io.CopyN(file, tarReader, maxFileSize+1)
			if err != nil && err != io.EOF {
				file.Close()
				return fmt.Errorf("failed to write file %s: %w", outputPath, err)
			}

			file.Close()

		case tar.TypeSymlink, tar.TypeLink:
			// Security: reject symlinks and hard links
			return fmt.Errorf("links not allowed in archive: %s", header.Name)

		default:
			// Skip other file types
			continue
		}
	}

	return nil
}
