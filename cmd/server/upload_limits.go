package main

import (
	"errors"
	"strconv"

	"magworkspace/internal/domain"
)

func configureUploadLimit() (int, error) {
	mb, err := strconv.Atoi(env("MAX_FILE_SIZE_MB", strconv.Itoa(domain.DefaultMaxFileSizeMB)))
	if err != nil || mb < 1 || mb > domain.MaxStoredDocumentBytes/(1<<20) {
		return 0, errors.New("MAX_FILE_SIZE_MB must be an integer between 1 and 512")
	}
	return mb * (1 << 20), nil
}
