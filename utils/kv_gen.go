package utils

import (
	"math/rand"
	"time"
)

func GenerateRandomEntry(store map[string]string) {
	// Generate a random string for the key
	key := generateRandomString(10)

	// Generate a random color from a predefined list
	colors := []string{"red", "green", "blue", "yellow", "orange", "purple", "pink", "brown", "black", "white"}
	color := colors[rand.Intn(len(colors))]

	// Store the key-value pair in the map
	store[key] = color
}

func generateRandomString(length int) string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, length)
	for i := range b {
		b[i] = chars[rng.Intn(len(chars))]
	}
	return string(b)
}
