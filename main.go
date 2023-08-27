package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gocql/gocql"
	// "time"
)
func connectToCassandra() (*gocql.Session, error) {
    cluster := gocql.NewCluster("127.0.0.1:9042")
    cluster.Keyspace = "user_service"
    cluster.Consistency = gocql.Quorum
    return cluster.CreateSession()
}

type User struct {
    UserID    gocql.UUID `json:"user_id"`
    Username  string     `json:"username"`
    Password  string     `json:"-"`
    Email     string     `json:"email"`
    CreatedAt time.Time  `json:"created_at"`
}


func getUser(session *gocql.Session) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        userID := r.URL.Query().Get("user_id")
        if userID == "" {
            http.Error(w, "Missing user_id", http.StatusBadRequest)
            return
        }

        // Parsing the UUID
        uuid, err := gocql.ParseUUID(userID)
        if err != nil {
            http.Error(w, "Invalid user_id", http.StatusBadRequest)
            return
        }

        var user User
        query := "SELECT user_id, username, password, email, created_at FROM users WHERE user_id = ? LIMIT 1"
        if err := session.Query(query, uuid).Consistency(gocql.One).Scan(&user.UserID, &user.Username, &user.Password, &user.Email, &user.CreatedAt); err != nil {
            http.Error(w, "User not found", http.StatusNotFound)
            return
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(user)
    }
}


func registerUser(session *gocql.Session) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        username := r.FormValue("username")
        password := r.FormValue("password")
        email := r.FormValue("email")

        if username == "" || password == "" || email == "" {
            http.Error(w, "Missing parameters", http.StatusBadRequest)
            return
        }

		currentTime := time.Now()


        userID := gocql.TimeUUID()
		if err := session.Query("INSERT INTO users (user_id, username, password, email, created_at) VALUES (?, ?, ?, ?, ?)",
		userID, username, password, email, currentTime).Consistency(gocql.One).Exec(); err != nil {
		log.Println("Error inserting user:", err)
		http.Error(w, "Server error", http.StatusInternalServerError)
		return
	}

        fmt.Fprintf(w, "User created with ID: %s", userID)
    }
}

func getAllUsers(session *gocql.Session) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var users []User

        query := "SELECT user_id, username, password, email, created_at FROM users"
        // iter := session.Query(query).Iter()
        iter := session.Query(query).Consistency(gocql.One).Iter()

        for {
            var user User
            if !iter.Scan(&user.UserID, &user.Username, &user.Password, &user.Email, &user.CreatedAt) {
                break
            }
            users = append(users, user)
        }

        if err := iter.Close(); err != nil {
            log.Println("Error closing iterator:", err)
            http.Error(w, "Server error", http.StatusInternalServerError)
            return
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(users)
    }
}


func main() {
    session, err := connectToCassandra()
    if err != nil {
        log.Fatalf("Could not connect to Cassandra: %v", err)
    }
    defer session.Close()

    http.HandleFunc("/register", registerUser(session))
	http.HandleFunc("/getallusers", getAllUsers(session))
    http.HandleFunc("/getuser", getUser(session))



    fmt.Println("Server running on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
