// dirscan/main.go
// Written using Claude from Anthropic - 2024-12-26
// This was written to test how fast a Raspberry Pi 4 B could write to a SQLite3 database that sits on an SD card
// By default, pragmas are setup wrong for speed, but correct for data recovery in case of failures, which wasn't a required test
// Running a scan on the /usr directory netted me about 120k files put into the database.  
// Without the pragmas set below, 17k entries were put into the database in about 5 minutes.
// With the pragmas, 130k were in (Dupes included) within 5 seconds
// @raspberrypi:~/go/dirscan $ time ./dirscan /usr/ dirscan.db3
// Directory scan completed successfully
// real    0m4.142s
// user    0m2.808s
// sys     0m1.405s

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func initDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	// Set performance parameters
	_, err = db.Exec(`PRAGMA synchronous = OFF`)
	if err != nil {
		return nil, fmt.Errorf("error setting synchronous pragma: %v", err)
	}
	
	_, err = db.Exec(`PRAGMA journal_mode = MEMORY`)
	if err != nil {
		return nil, fmt.Errorf("error setting journal_mode pragma: %v", err)
	}

	_, err = db.Exec(`PRAGMA cache_size = 100000`)
	if err != nil {
		return nil, fmt.Errorf("error setting cache_size pragma: %v", err)
	}

	// Create the files table if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		filepath TEXT NOT NULL,
		filename TEXT NOT NULL,
		size INTEGER NOT NULL,
		modified_time DATETIME NOT NULL,
		created_time DATETIME NOT NULL
	);`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return nil, fmt.Errorf("error creating table: %v", err)
	}

	return db, nil
}

func scanDirectory(path string, db *sql.DB) error {
	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback() // Will rollback if not committed

	// Prepare the insert statement once for the whole transaction
	stmt, err := tx.Prepare(`
		INSERT INTO files (filepath, filename, size, modified_time, created_time)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	err = filepath.Walk(path, func(filepath string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %q: %v", filepath, err)
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get file creation time (birth time) or modification time if creation time is not available
		var birthTime time.Time
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			// On Linux, birth time might not be available, falling back to ctime
			birthTime = time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec)
		} else {
			// Fallback to modification time if we can't get creation time
			birthTime = info.ModTime()
		}

		// Execute the insert
		_, err = stmt.Exec(
			filepath,
			info.Name(),
			info.Size(),
			info.ModTime(),
			birthTime,
		)
		if err != nil {
			return fmt.Errorf("error inserting record for %q: %v", filepath, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: program <directory_path> <database_path>")
	}

	dirPath := os.Args[1]
	dbPath := os.Args[2]

	// Initialize database
	db, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Scan directory and store file information
	err = scanDirectory(dirPath, db)
	if err != nil {
		log.Fatalf("Error scanning directory: %v", err)
	}

	fmt.Println("Directory scan completed successfully")
}
