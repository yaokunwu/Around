package main 

import (
    "path/filepath"
    "fmt"
    "log"
    "encoding/json"
    "net/http"
    "strconv"
    elastic "gopkg.in/olivere/elastic.v3"
    "github.com/pborman/uuid"
    "reflect"
    "context"
    "cloud.google.com/go/storage"
    "io"
    "github.com/auth0/go-jwt-middleware"
    "github.com/form3tech-oss/jwt-go"
    "github.com/gorilla/mux"
    "cloud.google.com/go/bigtable"

)

type Location struct {
    Lat float64 `json:"lat"`
    Lon float64 `json:"lon"`
}

type Post struct {
    User string `json:"user"`
    Message string `json:"message"`
    Location Location `json:"location"`
    Url string `json:"url"`
    Type string `json:"type"`
    Face float64 `json:"face"`
}

var (
    mediaTypes = map[string]string {
        ".jpeg": "image",
    }
    
)

const (
    DISTANCE = "200km"
    INDEX = "around"
    TYPE = "post"
    BUCKET_NAME="post-images-314619"
    // Needs to update
    PROJECT_ID = "around-314619"
    BT_INSTANCE = "around-post"
    // Needs to update this URL if you deploy it to cloud.
    ES_URL = "http://34.136.163.26:9200"

)

var mySigningKey = []byte("secret")


func main() {
    // Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

    fmt.Println("started-service")

    r := mux.NewRouter()


	jwtMiddleware := jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return mySigningKey, nil
		},
		// When set, the middleware verifies that tokens are signed with the specific signing algorithm
		// If the signing method is not constant the ValidationKeyGetter callback can be used to implement additional checks
		// Important to avoid security issues described here: https://auth0.com/blog/critical-vulnerabilities-in-json-web-token-libraries/
		SigningMethod: jwt.SigningMethodHS256,
	})

      r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
      r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
      r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
      r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

      http.Handle("/", r)
      log.Fatal(http.ListenAndServe(":8080", nil))

}


func handlerPost(w http.ResponseWriter, r *http.Request) {

    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "*")
    w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

   
    // "user" defined in jwt, to retrieve token
    user := r.Context().Value("user")
    claims := user.(*jwt.Token).Claims
    username := claims.(jwt.MapClaims)["username"]

    r.ParseMultipartForm(32 << 20)

    //Parse
    fmt.Printf("Received one post request %s\n", r.FormValue("message"))
    lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
    lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

    p := &Post {
        User: username.(string),
        Message: r.FormValue("message"),
        Location: Location {
            Lat: lat,
            Lon: lon,
        },
    }


    fmt.Fprintf(w, "Post received: %s\n", p.Message)

    id := uuid.New()
    
    file, _, err := r.FormFile("image")
    if err != nil {
        http.Error(w, "GCS is not setup 1111", http.StatusInternalServerError)
        fmt.Printf("GCS is not setup 1111%v\n", err)
        panic(err)
    }
    defer file.Close()

    ctx := context.Background()

    _, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
    if err != nil {
        http.Error(w, "GCS is not setup2222", http.StatusInternalServerError)
        fmt.Printf("GCS is not setup2222 %v\n", err)
        panic(err)
    }

    im, header, _ := r.FormFile("image")
    defer im.Close()
    suffix := filepath.Ext(header.Filename)

    if t, ok := mediaTypes[suffix]; ok {
        p.Type = t
    } else {
        p.Type = "unknown"
    }

    if suffix == ".jpeg" {
        if score, err := annotate(im); err != nil {
            http.Error(w, "Failed", http.StatusInternalServerError)
            fmt.Printf("Failed to annotate")
            return
        } else {
            p.Face = score
        }
    }

    p.Url = attrs.MediaLink
    
    saveToES(p, id)

//    saveToBigTable(p, id)
}


func saveToGCS(ctx context.Context, r io.Reader, bucketName, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
    client, err := storage.NewClient(ctx)
    if err != nil {
        return nil, nil, err
    }

    defer client.Close()

    bucket := client.Bucket(bucketName)
    // check whether bucket exists
    if _, err := bucket.Attrs(ctx); err != nil {
        return nil, nil, err
    }

    obj := bucket.Object(name)
    wc := obj.NewWriter(ctx)

    if _, err = io.Copy(wc, r); err != nil {
        return nil, nil, err
    }
    if err := wc.Close(); err != nil {
        return nil, nil, err
    }

    if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
        return nil, nil, err
    }

    attrs, err := obj.Attrs(ctx)
    fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)
    return obj, attrs, nil
}

func saveToBigTable(p *Post, id string) {
    ctx := context.Background()
    // you must update project name here
    bt_client, err := bigtable.NewClient(ctx, PROJECT_ID, BT_INSTANCE) 
    if err != nil {
        panic(err)
        return 
    }
    tbl := bt_client.Open("post") 
    mut := bigtable.NewMutation() 
    t := bigtable.Now()
    mut.Set("post", "user", t, []byte(p.User))
    mut.Set("post", "message", t, []byte(p.Message))
    mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))        
    mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))
    err = tbl.Apply(ctx, id, mut) 
    if err != nil {
        panic(err)
        return 
    }
    fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
}


func saveToES(p *Post, id string) {
    es_client, err := elastic.NewClient(elastic.SetURL(ES_URL),
                    elastic.SetSniff(false))
    if err != nil {
        panic(err)
    }

    _, err = es_client.Index().
            Index(INDEX).
            Type(TYPE).
            Id(id).
            BodyJson(p).
            Refresh(true).
            Do()

    if err != nil {
        panic(err)
    }

    fmt.Printf("Post is saved to index: %s\n", p.Message)
}


func handlerSearch(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Received one request for search.")

    lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
    lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)

    rang := DISTANCE
    if val := r.URL.Query().Get("range"); val != "" {
        rang = val + "km"
    }
    fmt.Printf("Search received: %f %f %s\n", lat, lon, rang)

    client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))

    if err != nil {
        panic(err)
    }

    q := elastic.NewGeoDistanceQuery("location")
    q = q.Distance(rang).Lat(lat).Lon(lon) 
    

    searchResult, err := client.Search().
        Index(INDEX).
        Query(q).
        Pretty(true).
        Do()
    if err != nil {
        panic(err)
    }

    fmt.Println("Query took %d millisecconds\n", searchResult.TookInMillis)
    fmt.Printf("Found a total of %d posts\n", searchResult.TotalHits())

    var typ Post
    var ps []Post
    for _, item := range searchResult.Each(reflect.TypeOf(typ)) {
        p := item.(Post)
        fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)

        ps = append(ps, p)
    }

    js, err := json.Marshal(ps)
    if err != nil {
        panic(err)
    }

    w.Header().Set("Content-Type","application/json")
    w.Header().Set("Access-Control-Allow-Origin", "*")
    w.Write(js)

}
