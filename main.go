package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
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

// type User struct {
//     UserID    gocql.UUID `json:"user_id"`
//     Username  string     `json:"username"`
//     Password  string     `json:"-"`
//     Email     string     `json:"email"`
//     CreatedAt time.Time  `json:"created_at"`
// }

type Response struct {
    UserID  string `json:"user_id"`
    Message string `json:"message"`
}

type User struct {
    // Existing Fields
    UserID    gocql.UUID `json:"user_id"`
    Username  string     `json:"username"`
    Password  string     `json:"-"`
    Email     string     `json:"email"`
    CreatedAt time.Time  `json:"created_at"`

    // New Fields
    FirstName                   string    `json:"first_name"`
    MiddleName                  string    `json:"middle_name"`
    LastName                    string    `json:"last_name"`
    Gender                      string    `json:"gender"`
    DOB                         time.Time `json:"dob"`
    PlaceOfBirth                string    `json:"place_of_birth"`
    Nationality                 string    `json:"nationality"`
    MaritalStatus               string    `json:"marital_status"`
    SpouseName                  string    `json:"spouse_name"`
    NationalID                  string    `json:"national_id"`
    PassportNumber              string    `json:"passport_number"`
    SSN                         string    `json:"ssn"`
    DrivingLicense              string    `json:"driving_license"`
    ResidentialAddress          string    `json:"residential_address"`
    MailingAddress              string    `json:"mailing_address"`
    PhoneNumber                 string    `json:"phone_number"`
    EmailAddress                string    `json:"email_address"`
    FathersName                 string    `json:"fathers_name"`
    MothersName                 string    `json:"mothers_name"`
    EmergencyContactName        string    `json:"emergency_contact_name"`
    EmergencyContactRelationship string    `json:"emergency_contact_relationship"`
    EmergencyContactPhone       string    `json:"emergency_contact_phone"`
    EmploymentStatus            string    `json:"employment_status"`
    CurrentEmployer             string    `json:"current_employer"`
    CurrentJobTitle             string    `json:"current_job_title"`
    EmploymentHistory           []string  `json:"employment_history"`
    HighestEducation            string    `json:"highest_education"`
    InstitutionsAttended        []string  `json:"institutions_attended"`
    GraduationYears             []string  `json:"graduation_years"`  // using string for simplification, you can use []time.Time based on your requirement
    BloodType                   string    `json:"blood_type"`
    KnownAllergies              []string  `json:"known_allergies"`
    MedicalHistory              string    `json:"medical_history"`
    FingerprintData             []byte    `json:"fingerprint_data"`
    RetinaScan                  []byte    `json:"retina_scan"`
    FacialData                  []byte    `json:"facial_data"`
    LanguagesSpoken             []string  `json:"languages_spoken"`
    CriminalRecord              bool      `json:"criminal_record"`
    SkillsOrQualifications      string    `json:"skills_or_qualifications"`
    MilitaryServiceRecord       string    `json:"military_service_record"`
    RegistrationDate            time.Time `json:"registration_date"`
    LastUpdated                 time.Time `json:"last_updated"`
    AuditLogs                   []string  `json:"audit_logs"`
    TermsAndConditionsAck       bool      `json:"terms_and_conditions_ack"`
    PrivacyPolicyAck            bool      `json:"privacy_policy_ack"`
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

func deleteUser(session *gocql.Session) http.HandlerFunc {
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

        // Delete the user from the database
        query := "DELETE FROM users WHERE user_id = ?"
        if err := session.Query(query, uuid).Consistency(gocql.One).Exec(); err != nil {
            http.Error(w, "Server error", http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusNoContent)
    }
}





func registerUser(session *gocql.Session) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
     

        username := r.FormValue("username")
        password := r.FormValue("password") 
        email := r.FormValue("email")
        first_name := r.FormValue("first_name")
        middle_name := r.FormValue("middle_name")
        last_name := r.FormValue("last_name")
        gender := r.FormValue("gender")
        dob := time.Now() //r.FormValue("dob")
        place_of_birth := r.FormValue("place_of_birth")
        nationality := r.FormValue("nationality")
        marital_status := r.FormValue("marital_status")
        spouse_name := r.FormValue("spouse_name")
        national_id := r.FormValue("national_id")
        passport_number := r.FormValue("passport_number")
        ssn := r.FormValue("ssn")
        driving_license := r.FormValue("driving_license")
        residential_address := r.FormValue("residential_address")
        mailing_address := r.FormValue("mailing_address")
        phone_number := r.FormValue("phone_number")
        email_address := r.FormValue("email")
        fathers_name := r.FormValue("fathers_name")
        mothers_name := r.FormValue("mothers_name")
        emergency_contact_name := r.FormValue("emergency_contact_name")
        emergency_contact_relationship := r.FormValue("emergency_contact_relationship")
        emergency_contact_phone := r.FormValue("emergency_contact_phone")
        employment_status := r.FormValue("employment_status")
        current_employer := r.FormValue("current_employer")
        current_job_title := r.FormValue("current_job_title")
        employment_history :=  []string{"job1", "job2"}//r.FormValue("employment_history") 
        highest_education := r.FormValue("highest_education")
        institutions_attended := []string{"BIT", "KIU"} //r.FormValue("institutions_attended")
        graduation_years := []time.Time{time.Now(), time.Now().AddDate(-1, 0, 0)} //r.FormValue("graduation_years")
        blood_type := r.FormValue("blood_type")
        known_allergies := []string{"log1", "log2"} //r.FormValue("known_allergies")
        medical_history := r.FormValue("medical_history")
        languages_spoken := []string{"lingagala", "eng"} //r.FormValue("languages_spoken")
        criminal_record := true //r.FormValue("criminal_record")
        skills_or_qualifications := r.FormValue("skills_or_qualifications")
        military_service_record := r.FormValue("military_service_record")
        terms_and_conditions_ack := true //r.FormValue("terms_and_conditions_ack") 
        privacy_policy_ack := true //r.FormValue("privacy_policy_ack")
        var retina_scan  []byte
        
         audit_logs := []string{"log1", "log2"}  //[]byte
        var facial_data []byte
        var fingerprint_data []byte
        // Check for mandatory fields bossa bossa
        if username == "" || password == "" || email == "" {
        http.Error(w, "Missing mandatory parameters", http.StatusBadRequest)
        return
        }

        // Generate UUID and timestamp
        user_id := gocql.TimeUUID()
        // currentTime := time.Now()
        created_at := time.Now()
        last_updated := time.Now()
        registration_date := time.Now()

// Insert query
    //     query := `
    //     INSERT INTO users (
    //         user_id, 
    //         blood_type, 
    //         created_at, 
    //         criminal_record, 
    //         current_employer,
    //         current_job_title, 
    //         dob, 
    //         driving_license, 
    //         email, email_address,
    //         emergency_contact_name,
    //          emergency_contact_phone, 
    //          emergency_contact_relationship,
    //         employment_status, 
    //         facial_data, 
    //         fathers_name, 
    //         fingerprint_data, 
    //         first_name,
    //         gender, 
    //         highest_education,  
    //         last_name, 
    //         last_updated, 
    //         mailing_address,
    //         marital_status,
    //          medical_history, 
    //          middle_name, 
    //          military_service_record,
    //         mothers_name, 
    //         national_id, 
    //         nationality, 
    //         passport_number, 
    //         password,
    //         phone_number, 
    //         place_of_birth,
    //          privacy_policy_ack, 
    //          registration_date,
    //         residential_address, 
    //         retina_scan, 
    //         skills_or_qualifications, 
    //         spouse_name, 
    //         ssn, 
    //         terms_and_conditions_ack, 
    //         username, 
    //         audit_logs, 
    //         employment_history,
    //         graduation_years, 
    //         institutions_attended,
    //          known_allergies, 
    //          languages_spoken
    //    )
    //     VALUES(?,?,?,?,?,?,?,?,?,?,  
    //         ?,?,?,?,?,?,?,?,?,?,
    //         ?,?,?,?,?,?,?,?,?,?,
    //         ?,?,?,?,?,?,?,?,?,?,
    //         ?,?,?,?,?,?,?,?,?)`

personalQuery :=  `
    INSERT INTO users(
    username,
    first_name,
    middle_name,
    last_name,
    email,
    phone_number,
    dob,
    place_of_birth,
    gender,
    nationality,
    marital_status,
    spouse_name,
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
`

        // Execute query
if err := session.Query(personalQuery,
            user_id, 
            blood_type, 
            created_at, 
            criminal_record, 
            current_employer,
            current_job_title, 
            dob, 
            driving_license, 
            email, 
            email_address,
            emergency_contact_name,
             emergency_contact_phone, 
             emergency_contact_relationship,
            employment_status, 
            facial_data, 
            fathers_name, 
            fingerprint_data, 
            first_name,
            gender, 
            highest_education, 
            last_name, 
            last_updated, 
            mailing_address,
            marital_status,
             medical_history, 
             middle_name, 
             military_service_record,
            mothers_name, 
            national_id, 
            nationality, 
            passport_number, 
            password,
            phone_number, 
            place_of_birth,
             privacy_policy_ack, 
             registration_date,
            residential_address, 
            retina_scan, 
            skills_or_qualifications, 
            spouse_name,
            ssn, 
            terms_and_conditions_ack, 
            username, 
            audit_logs, 
            employment_history,
            graduation_years, 
            institutions_attended,
             known_allergies, 
             languages_spoken ).Consistency(gocql.One).Exec(); err != nil {
    log.Println("Error inserting user:", err)
    http.Error(w, "Server error", http.StatusInternalServerError)
    return
}


        fmt.Fprintf(w, "User created with ID: %s", user_id)
    }
}

// Identification & Legal Status
func updateIdentity(session *gocql.Session) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Extract form data
        username := r.FormValue("username")
        email := r.FormValue("email")
        first_name := r.FormValue("first_name")
        middle_name := r.FormValue("middle_name")
        last_name := r.FormValue("last_name")
        gender := r.FormValue("gender")
        dob := time.Now()
        place_of_birth := r.FormValue("place_of_birth")
        nationality := r.FormValue("nationality")
        marital_status := r.FormValue("marital_status")
        spouse_name := r.FormValue("spouse_name")
        phone_number := r.FormValue("phone_number")
        
        // Basic validation
        if username == "" || email == "" {
            http.Error(w, "Missing mandatory parameters", http.StatusBadRequest)
            return
        }
        
        // Generate a new user_id
        user_id := gocql.TimeUUID()
        
        // CQL Query
        personalQuery := `
            INSERT INTO users(
                user_id,
                username,
                first_name,
                middle_name,
                last_name,
                email,
                phone_number,
                dob,
                place_of_birth,
                gender,
                nationality,
                marital_status,
                spouse_name
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`
        
        // Execute query
        if err := session.Query(personalQuery,
            user_id,
            username,
            first_name,
            middle_name,
            last_name,
            email,
            phone_number,
            dob,
            place_of_birth,
            gender,
            nationality,
            marital_status,
            spouse_name).Consistency(gocql.One).Exec(); err != nil {
            log.Println("Error inserting user:", err)
            http.Error(w, "Server error", http.StatusInternalServerError)
            return
        }
        
        // Create a Response instance and populate it
        response := Response{
            UserID:  user_id.String(),
            Message: "User created successfully",
        }
        
        // Convert the Response to JSON
        jsonResponse, err := json.Marshal(response)
        if err != nil {
            log.Println("Error marshalling JSON:", err)
            http.Error(w, "Server error", http.StatusInternalServerError)
            return
        }
        
        // Set the Content-Type and write the response
        w.Header().Set("Content-Type", "application/json")
        w.Write(jsonResponse)
    }
}





// legel in formation 
func updateAdditionalInfo(session *gocql.Session) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Extract the user_id and other form data
        user_id := r.FormValue("user_id")
        national_id := r.FormValue("national_id")
        passport_number := r.FormValue("passport_number")
        ssn := r.FormValue("ssn")
        driving_license := r.FormValue("driving_license")
        // criminal_record := r.FormValue("criminal_record")

        // strCriminalRecord := r.FormValue("criminal_record")
        // criminal_record := r.FormValue("criminal_record")

        // boolCriminalRecord, err := strconv.ParseBool(strCriminalRecord)
        // if err != nil {
        //     log.Println("Error parsing criminal_record:", err)
        //     http.Error(w, "Invalid value for criminal_record", http.StatusBadRequest)
        //     return
        // }
        // criminal_record := boolCriminalRecord
        // military_service_record := r.FormValue("military_service_record")
        // strMilitaryServiceRecord := r.FormValue("military_service_record")
        // boolMilitaryServiceRecord, err := strconv.ParseBool(strMilitaryServiceRecord)
        // if err != nil {
        //     log.Println("Error parsing military_service_record:", err)
        //     http.Error(w, "Invalid value for military_service_record", http.StatusBadRequest)
        //     return
        // }

        // military_service_record := boolMilitaryServiceRecord
        // Basic validation
        if user_id == "" {
            http.Error(w, "Missing mandatory user_id", http.StatusBadRequest)
            return
        }
        
        // Prepare CQL Query to update the additional information
        additionalInfoQuery := `
            UPDATE users SET
                national_id = ?,
                passport_number = ?,
                ssn = ?,
                driving_license = ?
                
            WHERE user_id = ?`
        
        // Convert user_id from string to UUID
        userIDUUID, err := gocql.ParseUUID(user_id)
        if err != nil {
            log.Println("Error parsing UUID:", err)
            http.Error(w, "Invalid user_id", http.StatusBadRequest)
            return
        }
        
        // Execute the query
        if err := session.Query(additionalInfoQuery,
            national_id,
            passport_number,
            ssn,
            driving_license,
            userIDUUID).Consistency(gocql.One).Exec(); err != nil {
            log.Println("Error updating user:", err)
            http.Error(w, "Server error", http.StatusInternalServerError)
            return
        }
        
        // Respond to the client
        // fmt.Fprintf(w, "User with ID: %s updated successfully", user_id)

        // Create a Response instance and populate it
        response := Response{
            UserID:  user_id,
            Message: "User created successfully",
        }
        
        // Convert the Response to JSON
        jsonResponse, err := json.Marshal(response)
        if err != nil {
            log.Println("Error marshalling JSON:", err)
            http.Error(w, "Server error", http.StatusInternalServerError)
            return
        }
        
        // Set the Content-Type and write the response
        w.Header().Set("Content-Type", "application/json")
        w.Write(jsonResponse)
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


func updateContactAndEmergencyDetails(session *gocql.Session) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Extract form data
        user_id := r.FormValue("user_id")
        residential_address := r.FormValue("residential_address")
        mailing_address := r.FormValue("mailing_address")
        email_address := r.FormValue("email_address")
        emergency_contact_name := r.FormValue("emergency_contact_name")
        emergency_contact_relationship := r.FormValue("emergency_contact_relationship")
        emergency_contact_phone := r.FormValue("emergency_contact_phone")

        // Basic validation
        if user_id == "" {
            http.Error(w, "Missing mandatory user_id", http.StatusBadRequest)
            return
        }

        // Prepare CQL Query
        updateQuery := `
            UPDATE users SET
                residential_address = ?,
                mailing_address = ?,
                email_address = ?,
                emergency_contact_name = ?,
                emergency_contact_relationship = ?,
                emergency_contact_phone = ?
            WHERE user_id = ?`

        // Convert user_id from string to UUID
        userIDUUID, err := gocql.ParseUUID(user_id)
        if err != nil {
            log.Println("Error parsing UUID:", err)
            http.Error(w, "Invalid user_id", http.StatusBadRequest)
            return
        }

        // Execute the query
        if err := session.Query(updateQuery,
            residential_address,
            mailing_address,
            email_address,
            emergency_contact_name,
            emergency_contact_relationship,
            emergency_contact_phone,
            userIDUUID).Consistency(gocql.One).Exec(); err != nil {
            log.Println("Error updating user:", err)
            http.Error(w, "Server error", http.StatusInternalServerError)
            return
        }

        // Create a Response instance and populate it
        response := Response{
            UserID:  user_id,
            Message: "User updated successfully",
        }

        // Convert the Response to JSON
        jsonResponse, err := json.Marshal(response)
        if err != nil {
            log.Println("Error marshalling JSON:", err)
            http.Error(w, "Server error", http.StatusInternalServerError)
            return
        }

        // Set the Content-Type and write the response
        w.Header().Set("Content-Type", "application/json")
        w.Write(jsonResponse)
    }
}

func updateEmploymentAndEducation(session *gocql.Session) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract form data
		user_id := r.FormValue("user_id")
		employment_status := r.FormValue("employment_status")
		current_job_title := r.FormValue("current_job_title")
		employment_history := strings.Split(r.FormValue("employment_history"), ",")
		highest_education := r.FormValue("highest_education")
		institutions_attended := strings.Split(r.FormValue("institutions_attended"), ",")
		graduation_years := []time.Time{time.Now(), time.Now().AddDate(-1, 0, 0)} 
		skills_or_qualifications := r.FormValue("skills_or_qualifications")

        

		// Basic validation
		if user_id == "" {
			http.Error(w, "Missing mandatory user_id", http.StatusBadRequest)
			return
		}

		// Prepare CQL Query
		updateQuery := `
            UPDATE users SET
                employment_status = ?,
                current_job_title = ?,
                employment_history = ?,
                highest_education = ?,
                institutions_attended = ?,
                graduation_years = ?,
                skills_or_qualifications = ?
            WHERE user_id = ?`

		// Convert user_id from string to UUID
		userIDUUID, err := gocql.ParseUUID(user_id)
		if err != nil {
			log.Println("Error parsing UUID:", err)
			http.Error(w, "Invalid user_id", http.StatusBadRequest)
			return
		}

		// Execute the query
		if err := session.Query(updateQuery,
			employment_status,
			current_job_title,
			employment_history,
			highest_education,
			institutions_attended,
			graduation_years,
			skills_or_qualifications,
			userIDUUID).Consistency(gocql.One).Exec(); err != nil {
			log.Println("Error updating user:", err)
			http.Error(w, "Server error", http.StatusInternalServerError)
			return
		}

		// Create a Response instance and populate it
		response := Response{
			UserID:  user_id,
			Message: "User updated successfully",
		}

		// Convert the Response to JSON
		jsonResponse, err := json.Marshal(response)
		if err != nil {
			log.Println("Error marshalling JSON:", err)
			http.Error(w, "Server error", http.StatusInternalServerError)
			return
		}

		// Set the Content-Type and write the response
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
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
    http.HandleFunc("/deleteuser", deleteUser(session))

// updateIdentity
    http.HandleFunc("/registerpersional", updateIdentity(session))
// updateAdditionalInfo
http.HandleFunc("/updateAdditionalInfo", updateAdditionalInfo(session))
// updateContactAndEmergencyDetails
http.HandleFunc("/updatecontactInfo", updateContactAndEmergencyDetails(session))
// updateEmploymentAndEducation
http.HandleFunc("/updateemplotInfo", updateEmploymentAndEducation(session))


    fmt.Println("Server running on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
