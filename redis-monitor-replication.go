package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	redis "github.com/go-redis/redis/v8"
)

var (
	redisHost = flag.String("redis-host", "localhost", "destination redis host")
	redisPort = flag.Int("redis-port", 6379, "destination redis port")
	redisDb   = flag.Int("redis-db", 0, "destination redis database number")
	enableLog = flag.Bool("log", false, "log all replicated commands")
)

const chanBuffer = 100

type RedisCommand struct {
	Name  string
	Args  [][]byte
	IArgs []interface{}
	Raw   string
}

// readMonitorOutput reads STDIN and produces a stream of RedisCommand structures.
func readMonitorOutput() chan RedisCommand {
	out := make(chan RedisCommand, chanBuffer)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			// 1592134898.858273 [0 192.168.23.10:33072] "HSET" "wh:7134878504547625" "207108" "^\xe6\x0c\xf2\x16\xab"
			//
			// actual command starts after first ']'
			i := strings.IndexByte(line, ']')
			if i < 0 {
				continue
			}
			// skip the bracket and space after it
			i += 2
			if i >= len(line) {
				continue
			}
			line = line[i:]
			cmd := parseRedisCommand(line)
			if cmd.Name == "" {
				continue
			}
			out <- cmd
		}
	}()
	return out
}

// parseRedisCommand handles un-quotation and un-escaping of redis command arguments
func parseRedisCommand(line string) RedisCommand {
	parts := [][]byte{}
	bs := []byte(line)
	inQuotes := false
	part := make([]byte, 0, len(bs))
	i := 0
	for i < len(bs) {
		c := bs[i]
		i++
		if inQuotes {
			switch c {
			case '"':
				inQuotes = false
				newPart := make([]byte, len(part))
				copy(newPart, part)
				parts = append(parts, newPart)
			case '\\':
				c := bs[i]
				i++
				switch c {
				case 'x':
					hexNum := bs[i : i+2]
					ord, err := strconv.ParseInt(string(hexNum), 16, 16)
					if err != nil {
						log.Printf("BAD HEX NUMBER %s: %v", string(hexNum), err)
					} else {
						part = append(part, byte(ord))
					}
					i += 2
				case '"', '\\':
					part = append(part, c)
				case 'r':
					part = append(part, 13)
				case 'a':
					part = append(part, 7)
				case 'b':
					part = append(part, 8)
				case 'n':
					part = append(part, 10)
				case 't':
					part = append(part, 9)
				default:
					log.Printf("UNEXPECTED ESCAPED CHAR %v %s", c, string([]byte{c}))
				}

			default:
				part = append(part, c)
			}
		} else {
			if c == '"' {
				inQuotes = true
				part = part[0:0]
			}
		}
	}
	var cmd RedisCommand
	cmd.Raw = line
	if len(parts) < 1 {
		return cmd
	}
	cmd.Name = string(parts[0])
	cmd.Args = parts[1:]
	cmd.IArgs = make([]interface{}, len(parts))
	for i := range parts {
		cmd.IArgs[i] = interface{}(parts[i])
	}

	return cmd
}

func main() {
	flag.Parse()

	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", *redisHost, *redisPort),
		DB:   *redisDb,
	})
	err := rdb.Ping(ctx).Err()
	if err != nil {
		log.Fatalf("redis connection error: %v", err)
	}

	for cmd := range readMonitorOutput() {
		if *enableLog {
			log.Printf("%v", cmd.Raw)
		}
		c := rdb.Do(ctx, cmd.IArgs...)
		err = c.Err()
		if err != nil {
			log.Printf("Error while executing command %s: %v", cmd.Raw, err)
		}
	}
}
