package main

import (
	"bitcask-go/store"
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	intro := "\n ▗▄▄▖▗▄▄▄▖▗▖  ▗▖▗▄▄▄▖ ▗▄▄▖▗▄▄▄▖ ▗▄▄▖\n▐▌   ▐▌   ▐▛▚▖▐▌▐▌   ▐▌     █  ▐▌   \n▐▌▝▜▌▐▛▀▀▘▐▌ ▝▜▌▐▛▀▀▘ ▝▀▚▖  █   ▝▀▚▖\n▝▚▄▞▘▐▙▄▄▖▐▌  ▐▌▐▙▄▄▖▗▄▄▞▘▗▄█▄▖▗▄▄▞▘\n                                    \n                                    \n                                    "

	commands := "Commands:\n" +
		"\t- set     <key> <value>   : insert a key-value pair\n" +
		"\t- get     <key>           : get a key value\n" +
		"\t- del     <key>           : delete a key\n" +
		"\t- ctrl+c                  : exit\n" +
		"\t- help                    : show this message"

	store, _ := store.NewDiskStore()

	fmt.Println(intro)
	fmt.Println(commands)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nEnter command: ")
		scanner.Scan()
		args := strings.Split(scanner.Text(), " ")

		switch args[0] {
		case "set":
			if len(args) != 3 {
				log.Fatal("Insufficient num of args")
			} else {
				key := args[1]
				val := args[2]
				store.Put(&key, &val)
			}
		case "get":
			if len(args) != 2 {
				log.Fatal("Insufficient num of args")
			} else {
				key := args[1]
				res, _ := store.Get(key)
				fmt.Println(res)
			}
		case "del":
			if len(args) != 2 {
				log.Fatal("Insufficient num of args")
			} else {
				key := args[1]
				err := store.Delete(key)
				if err != nil {
					fmt.Println("err: could not del key")
				} else {
					fmt.Println("deletion: success")
				}
			}
		case "help":
			fmt.Println("\n" + commands)
		}

	}

	//entries := make(map[string]string)
	//entries2 := make(map[string]string)
	//entries3 := make(map[string]string)

	// 100 entries = 1 table
	//for i := 0; i < 400; i++ {
	//	if i == 53 {
	//		entries["Foxtrot"] = "purple"
	//	} else {
	//		generateRandomEntry(entries)
	//	}
	//}
	//
	//for key, value := range entries {
	//	store.Put(key, value)
	//}
	//fmt.Println("--------------------------------------------------------")
	//
	//for i := 0; i < 1300; i++ {
	//	generateRandomEntry(entries2)
	//}
	//for key, value := range entries2 {
	//	store.Put(key, value)
	//}
	//
	//res, _ := store.Get("Foxtrot")
	//fmt.Println(res)

}

//func generateRandomEntry(store map[string]string) {
//	// Generate a random string for the key
//	key := generateRandomString(10)
//
//	// Generate a random color from a predefined list
//	colors := []string{"red", "green", "blue", "yellow", "orange", "purple", "pink", "brown", "black", "white"}
//	color := colors[rand.Intn(len(colors))]
//
//	// Store the key-value pair in the map
//	store[key] = color
//}

//func generateRandomString(length int) string {
//	rand.Seed(time.Now().UnixNano())
//	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
//
//	b := make([]rune, length)
//	for i := range b {
//		b[i] = chars[rand.Intn(len(chars))]
//
//	}
//	return string(b)
//}
