package main

import (
	"database/sql"

	"encoding/json"

	"fmt"

	"log"

	"context"

	"time"

	_ "github.com/ClickHouse/clickhouse-go"

	"github.com/go-redis/redis/v8"
)

type MyData struct {
	Col1 string

	Col2 string

	Col3 string
}

func main() {

	// Initialize a Redis client

	rdb := redis.NewClient(&redis.Options{

		Addr: "localhost:6379", // Redis server address

		Password: "", // No password for this example

		DB: 0, // Default DB

	})

	// Close the Redis client when the program exits

	defer rdb.Close()

	// Context for Redis operations

	ctx := context.Background()

	// Key for caching the query result

	cacheKey := "hello"

	// Check if the data is already cached in Redis

	cachedResult, err := rdb.Get(ctx, cacheKey).Result()

	if err == nil {

		// Data was found in the cache, so deserialize it

		var data []MyData

		err := json.Unmarshal([]byte(cachedResult), &data)

		if err != nil {

			fmt.Println("Error deserializing data from Redis:", err)

		} else {

			fmt.Println("Results from Redis cache:")

			for _, d := range data {

				fmt.Println(d)

			}

		}

	} else if err == redis.Nil {

		// Data was not found in the cache, so fetch it from ClickHouse

		clickHouseResult := fetchFromClickHouse()

		// Serialize the result and store it in Redis with an expiration time (e.g., 5 minutes)

		clickHouseResultJSON, err := json.Marshal(clickHouseResult)

		if err != nil {

			fmt.Println("Error serializing data:", err)

		} else {

			err := rdb.Set(ctx, cacheKey, clickHouseResultJSON, 5*time.Minute).Err()

			if err != nil {

				fmt.Println("Error caching data in Redis:", err)

			}

			// Use the ClickHouse result

			fmt.Println("Results from ClickHouse:")

			for _, d := range clickHouseResult {

				fmt.Println(d)

			}

		}

	} else {

		fmt.Println("Error checking Redis cache:", err)

	}

}

func fetchFromClickHouse() []MyData {

	// Open a connection to ClickHouse

	conn, err := sql.Open("clickhouse", "tcp://localhost:9000?database=mysdatabase&username=default&password=dona1502")

	if err != nil {

		log.Fatal(err)

	}

	defer conn.Close()

	// Execute a query

	query := "SELECT name,email,password FROM people" // Replace with your actual ClickHouse query

	rows, err := conn.QueryContext(context.Background(), query)

	if err != nil {

		log.Fatal(err)

	}

	defer rows.Close()

	// Process the query result and build a slice of MyData

	var result []MyData

	for rows.Next() {

		var col1 string

		var col2 string

		var col3 string

		if err := rows.Scan(&col1, &col2, &col3); err != nil {

			log.Fatal(err)

		}

		data := MyData{Col1: col1, Col2: col2, Col3: col3}

		result = append(result, data)

	}

	return result

}
