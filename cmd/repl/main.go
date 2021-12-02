package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/xyproto/ask"
	"github.com/xyproto/simplehstore"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	} else {
		fmt.Println("SUCCESS")
	}
}

func checkStringError(s string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	} else {
		fmt.Println(s)
	}
}

func checkBoolError(b bool, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	} else {
		fmt.Println(b)
	}
}

func checkSliceError(xs []string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	} else {
		fmt.Println(strings.Join(xs, "\n"))
	}
}

func checkMapError(m map[string]string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	} else {
		for k, v := range m {
			fmt.Printf("%s -> %s\n", k, v)
		}
	}
}

func main() {

	host := simplehstore.New() // locally
	defer host.Close()

	host.SetRawUTF8(true)

	simplehstore.Verbose = true
	hashmap, err := simplehstore.NewHashMap2(host, "devices")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

LOOP:
	for {
		cmd := ask.Ask("> ")
		fields := strings.Fields(cmd)
		if len(fields) > 0 {
			switch fields[0] {
			case "all":
				if len(fields) == 1 {
					checkSliceError(hashmap.All())
				} else {
					fmt.Println("all")
				}
			case "clear":
				if len(fields) == 1 {
					checkError(hashmap.Clear())
				} else {
					fmt.Println("clear")
				}
			case "exists":
				if len(fields) == 2 {
					checkBoolError(hashmap.Exists(fields[1]))
				} else {
					fmt.Println("exists o")
				}
			case "has":
				if len(fields) == 3 {
					checkBoolError(hashmap.Has(fields[1], fields[2]))
				} else {
					fmt.Println("has o k")
				}
			case "keys":
				if len(fields) == 2 {
					checkSliceError(hashmap.Keys(fields[1]))
				} else {
					fmt.Println("keys o")
				}
			case "l", "ls":
				if len(fields) == 1 {
					checkSliceError(hashmap.All())
				} else if len(fields) == 2 {
					if keys, err := hashmap.Keys(fields[1]); err != nil {
						checkError(err)
					} else {
						for _, k := range keys {
							if v, err := hashmap.Get(fields[1], k); err != nil {
								checkError(err)
							} else {
								fmt.Printf("%s -> %s\n", k, v)
							}
						}
					}
				} else if len(fields) == 3 {
					checkStringError(hashmap.Get(fields[1], fields[2]))
				} else {
					fmt.Println("l")
					fmt.Println("l o")
					fmt.Println("l o k")
				}
			case "p", "props", "encounteredkeys":
				if len(fields) == 1 {
					checkSliceError(hashmap.AllEncounteredKeys())
				} else {
					fmt.Println("p")
				}
			case "get":
				if len(fields) == 3 {
					checkStringError(hashmap.Get(fields[1], fields[2]))
				} else {
					fmt.Println("get o k")
				}
			case "getmap":
				if len(fields) >= 3 {
					checkMapError(hashmap.GetMap(fields[1], fields[2:]))
				} else {
					fmt.Println("getmap o k k k ...")
				}
			case "set":
				if len(fields) == 4 {
					checkError(hashmap.Set(fields[1], fields[2], fields[3]))
				} else {
					fmt.Println("set o k v")
				}
			case "setmap":
				if len(fields) >= 3 {
					m := make(map[string]string)
					for _, keyAndValue := range fields[2:] {
						keyValueFields := strings.SplitN(keyAndValue, ",", 2)
						if len(keyValueFields) == 2 {
							m[keyValueFields[0]] = keyValueFields[1]
						}
					}
					fmt.Println(m)
					checkError(hashmap.SetMap(fields[1], m))
				} else {
					fmt.Println("setmap o k,v k,v k,v ...")
				}
			case "setlargemap":
				if len(fields) >= 2 {
					m := make(map[string]map[string]string)
					for _, okv := range fields[1:] {
						ownerKeyValue := strings.SplitN(okv, ",", 3)
						if len(ownerKeyValue) == 3 {
							o := ownerKeyValue[0]
							k := ownerKeyValue[1]
							v := ownerKeyValue[2]
							if _, ok := m[o]; ok {
								m[o][k] = v
							} else {
								m[o] = make(map[string]string)
								m[o][k] = v
							}
						}
					}
					fmt.Println(m)
					checkError(hashmap.SetLargeMap(m))
				} else {
					fmt.Println("setlargemap o,k,v o,k,v o,k,v ...")
				}
			case "remove":
				if len(fields) == 1 {
					checkError(hashmap.Remove())
				} else {
					fmt.Println("remove")
				}
			case "exit", "quit":
				fmt.Println(strings.Title(fields[0]))
				break LOOP
			case "help", "?", "h":
				fmt.Println("all - list all owners")
				fmt.Println("clear - remove all data in this table")
				fmt.Println("exists o - check if owner exists")
				fmt.Println("exit - exit the repl")
				fmt.Println("get o k - get the value of a key of an owner")
				fmt.Println("getmap o k k k ... - from an owner, get a map of keys and values")
				fmt.Println("has o k - check if an owner has a key")
				fmt.Println("help - this text")
				fmt.Println("keys o - list all keys for an owner")
				fmt.Println("l [o] [k] - list all owners, all keys for an owner or the value for a key")
				fmt.Println("p - list all encountered properties")
				fmt.Println("remove - remove the table")
				fmt.Println("set o k v - set an owner's key to the given value")
				fmt.Println("setmap o k,v k,v k,v ... ")
				fmt.Println("setlargemap o,k,v o,k,v o,k,v ... ")
				fmt.Println("quit - exit the repl")
			default:
				fmt.Println("unrecognized command")
			}
		}
	}
}
