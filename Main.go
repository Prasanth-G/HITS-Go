package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"github.com/bbalet/stopwords"
)

const (
	LINKS_CHANNEL_LENGTH = 500
	NO_OF_CRAWLERS       = 10
)

type hashLinkPair struct {
	hash uint64
	link string
}

type homepagePlusLinks struct {
	homepage uint64
	keywords []string
	links    map[uint64]string
}

func HashValueOf(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func IsHtmlpage(url string) bool {
	client := &http.Client{}
	request, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false
	}
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	return strings.Contains(response.Header.Get("Content-Type"), "text/html")
}

func GetHyperlinks(requestURLChan <-chan string, pagePluslinks chan<- homepagePlusLinks) {

	for url := range requestURLChan {
		keywords := make([]string, 0)
		knownlinks := make(map[uint64]string)
		reg, err := regexp.Compile("[^a-zA-Z0-9]+")

		for _, each := range strings.Split(strings.Trim(stopwords.CleanString(reg.ReplaceAllString(url, " "), "en", true), " "), " ") {
			keywords = append(keywords, each)
		}

		doc, err := goquery.NewDocument(url)
		if err != nil {
			fmt.Println(err)
			pagePluslinks <- homepagePlusLinks{HashValueOf(url), keywords, knownlinks}
		}

		var hashvalue uint64
		var result []string
		homepage := strings.Join(strings.Split(url, "/")[0:3], "/")
		re, _ := regexp.Compile("^[A-Za-z0-9-_/.~]*")

		//Extracting link
		doc.Find("body a").Each(func(index int, item *goquery.Selection) {

			linkTag := item
			link, _ := linkTag.Attr("href")
			link = strings.Trim(link, " ")
			if len(link) > 0 && link[0] != byte('#') && link[0] != byte('?') {
				if !(strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://")) {
					result = re.FindAllString(link, -1)
					if len(result) == 1 {
						link = result[0]
					}
					//if it is relative to home page
					if strings.HasPrefix(link, "//") {
						link = "http:" + link
					} else if strings.HasPrefix(link, "/") {
						link = homepage + link
					} else if unicode.IsLetter(rune(link[0])) {
						link = url + "/" + link
					}
				}
				link = strings.TrimRight(link, "/")
				if len(link) > 0 && link[0] != byte('.') {

					hashvalue = HashValueOf(link)
					if _, duplicate := knownlinks[hashvalue]; !duplicate {
						knownlinks[hashvalue] = link
					}
				}
			}
			/*if len(result) == 1 {
				link = result[0]
			}
			if !(strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://")) {
				if strings.HasPrefix(link, "//") {
					link = "http:" + link
				} else if strings.HasPrefix(link, "/") {
					link = homepage + link
				} else if len(link) != 0 && unicode.IsLetter(rune(link[0])) {
					link = url + link
				}
			}*/
		})
		//extraction keywords
		doc.Find("body h1").Each(func(index int, item *goquery.Selection) {
			for _, each := range strings.Split(strings.Trim(stopwords.CleanString(reg.ReplaceAllString(item.Text(), " "), "en", true), " "), " ") {
				keywords = append(keywords, each)
			}
		})
		pagePluslinks <- homepagePlusLinks{HashValueOf(url), keywords, knownlinks}
	}
}

/*
func GetHyperlinks(requestURLChan <-chan string, pagePluslinks chan<- homepagePlusLinks) {

	client := &http.Client{}
	for url := range requestURLChan {
		knownlinks := make(map[uint64]string)
		request, err := http.NewRequest("GET", url, nil)
		if err != nil {
			fmt.Println("Error in request", url)
			pagePluslinks <- homepagePlusLinks{HashValueOf(url), knownlinks}
		}
		response, err := client.Do(request)
		if err != nil {
			fmt.Println("Error in getting response", url)
			pagePluslinks <- homepagePlusLinks{HashValueOf(url), knownlinks}
		}
		defer response.Body.Close()

		tokenizer := html.NewTokenizer(response.Body)
		var tokenname html.TokenType
		var token html.Token
		var hashvalue uint64
		var result []string
		homepage := strings.Join(strings.Split(response.Request.URL.String(), "/")[0:3], "/")
		re, _ := regexp.Compile("^[A-Za-z0-9-_/.~]*")
	outerfor:
		for {
			tokenname = tokenizer.Next()
			switch tokenname {
			case html.ErrorToken:
				break outerfor
			case html.StartTagToken:
				token = tokenizer.Token()
				if token.Data == "a" {
					for _, attr := range token.Attr {
						if attr.Key == "href" && len(attr.Val) != 0 && attr.Val[0] != byte('#') {
							result = re.FindAllString(attr.Val, -1)
							if len(result) == 1 {
								attr.Val = result[0]
							}

							//if it is relative to home page
							if strings.HasPrefix(attr.Val, "/") {
								attr.Val = homepage + attr.Val
							} else if unicode.IsLetter(rune(attr.Val[0])) {
								attr.Val = response.Request.URL.String() + attr.Val
							} else {
								continue
							}
							attr.Val = strings.TrimRight(attr.Val, "/")

							hashvalue = HashValueOf(attr.Val)
							if _, duplicate := knownlinks[hashvalue]; !duplicate {
								knownlinks[hashvalue] = attr.Val
							}
						}
					}
				}
			}
		}
		pagePluslinks <- homepagePlusLinks{HashValueOf(url), knownlinks}
	}
}*/

/*
func StoreInDB(waitgroup *sync.WaitGroup, links <-chan hashLinkPair) {
	for {
		hashLinkPair := <-links
		if hashLinkPair.link != "EOF" {
			fmt.Println(hashLinkPair.hash, hashLinkPair.link)
		} else {
			defer waitgroup.Done()
			break
		}
	}
}
*/

func main() {
	//url := "https://golang.org/pkg/net/http/"
	//url = "https://en.wikipedia.org/wiki/Binary_search_tree"
	url := "http://www.geeksforgeeks.org/fundamentals-of-algorithms/"
	indexes := make(map[uint64]string)
	toCrawl := make([]uint64, 0)
	adjList := make(map[uint64][]uint64)
	keywordMap := make(map[string][]uint64)

	pointedBy := make(map[uint64][]uint64)

	toCrawl_ := make(map[uint64]bool)
	//byt := []byte(`{"10006907394855601145":"https://golang.org/pkg/net/http/cgi/http"}`)
	//load stored data in to indexes, tocrawl, adjlist
	var offline string
	var limit int
	flag.StringVar(&offline, "offline", "yes", "If indexes.json, toCrawl.json, adjList.json not exist")
	flag.IntVar(&limit, "limit", 100, "Max limit on Number of web pages to crawl")
	flag.Parse()

	if offline == "yes" {

		indexesRead, err := ioutil.ReadFile("indexes.json")
		if err != nil {
			fmt.Println(err)
		}
		err = json.Unmarshal(indexesRead, &indexes)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Indexes Map Loaded")

		toCrawlRead, err := ioutil.ReadFile("toCrawl.json")
		if err != nil {
			fmt.Println(err)
		}
		err = json.Unmarshal(toCrawlRead, &toCrawl_)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("toCrawl list Loaded")

		for each := range toCrawl_ {
			toCrawl = append(toCrawl, each)
		}

		adjListRead, err := ioutil.ReadFile("adjList.json")
		if err != nil {
			fmt.Println(err)
		}
		err = json.Unmarshal(adjListRead, &adjList)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("adjList Map loaded")

		keywordMapRead, err := ioutil.ReadFile("keywordMap.json")
		if err != nil {
			fmt.Println(err)
		}
		err = json.Unmarshal(keywordMapRead, &keywordMap)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Keyword Map Loaded")

		pointedByRead, err := ioutil.ReadFile("pointedBy.json")
		if err != nil {
			fmt.Println(err)
		}
		err = json.Unmarshal(pointedByRead, &pointedBy)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("PointedBy Map Loaded")

	} else {
		//add seed url[s]
		h := HashValueOf(url)
		indexes[h] = url
		toCrawl = append(toCrawl, h)
	}
	fmt.Println("INDEXES ", len(indexes))
	fmt.Println("TOCRAWL ", len(toCrawl))
	fmt.Println("ADJLIST ", len(adjList))
	fmt.Println("KEYWORD ", len(keywordMap))
	fmt.Println("PointedBy", len(pointedBy))

	//var inp string
	//fmt.Println("START ??????")
	//fmt.Scanln(&inp)

	pagePlusLinks := make(chan homepagePlusLinks, LINKS_CHANNEL_LENGTH)
	requestURLChan := make(chan string, 10000)
	//var waitgroup sync.WaitGroup

	//add Threads to Threadpool
	for i := 0; i < NO_OF_CRAWLERS; i++ {
		go GetHyperlinks(requestURLChan, pagePlusLinks)
	}

	//add seed url[s]
	/*h := HashValueOf(url)
	indexes[h] = url
	toCrawl = append(toCrawl, h)*/

	var currentPage string
	var currentPageHash uint64
	//add request url to requestURLChan
	count := len(adjList)

	go func() {
		for {
			if len(toCrawl) == 0 {
				time.Sleep(time.Second * 5)
				continue
			}
			if count >= limit {
				break
			}

			currentPageHash, toCrawl = toCrawl[0], toCrawl[1:]
			if _, ok := indexes[currentPageHash]; !ok {
				fmt.Println("Url doesn't exist for ", currentPageHash)
				continue
			}
			currentPage = indexes[currentPageHash]

			//crawl the page
			if IsHtmlpage(currentPage) {
				requestURLChan <- currentPage
				count++
			} else {
				continue
			}

			//update : increment / decrement sleep time according to length of the requestchan
			time.Sleep(time.Second / 2)
		}
		close(requestURLChan)
	}()

	fmt.Println("Count , Limit ", count, limit)
	//waitgroup.Add(1)
	//go func() {
	for i := count; i < limit; i++ {
		ppl := <-pagePlusLinks
		fmt.Print(i, ", ")
		for linkHash, linkURL := range ppl.links {

			//add (hash, url) to indexes
			indexes[linkHash] = linkURL

			//add neighbours of homepage
			adjList[ppl.homepage] = append(adjList[ppl.homepage], linkHash)

			//add pointedby links
			pointedBy[linkHash] = append(pointedBy[linkHash], ppl.homepage)

			//if the link is not traversed it is not in adjList
			if _, present := adjList[linkHash]; !present {
				toCrawl = append(toCrawl, linkHash)
			}
		}
		for _, keyword := range ppl.keywords {
			keywordMap[keyword] = append(keywordMap[keyword], ppl.homepage)
		}
	}

	//}()
	//fmt.Println("waiting for goroutines...")
	//waitgroup.Wait()
	fmt.Println("INDEXES ", len(indexes))
	fmt.Println("TOCRAWL ", len(toCrawl))
	fmt.Println("ADJLIST ", len(adjList))

	fmt.Println("Calculating PageRank ...")

	//pagerank
	rankOf := make(map[uint64]float64)
	d := 0.85
	noOfIterations := 50
	noOfPages := float64(len(adjList))
	for each := range adjList {
		rankOf[each] = 1.0 / noOfPages
	}
	for i := 0; i < noOfIterations; i++ {
		newranks := make(map[uint64]float64)
		for eachPage := range adjList {
			newrank := (1.0 - d) / float64(noOfPages)
			for node := range adjList {
				if _, ok := adjList[node]; ok {
					newrank += d * (rankOf[node] / float64((len(adjList[node]))))
				}

			}
			newranks[eachPage] = newrank
		}

		for i, j := range newranks {
			newranks[i] = 0.2 * j
		}
		rankOf = newranks
	}

	//save to file
	fmt.Println("Writing data to files")
	indexesMars, _ := json.Marshal(indexes)
	toCrawlMars, _ := json.Marshal(toCrawl_)
	adjListMars, _ := json.Marshal(adjList)
	keywordMapMars, _ := json.Marshal(keywordMap)
	pointedByMars, _ := json.Marshal(pointedBy)

	indexesFile, err := os.OpenFile("indexes.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Println("Error opening file")
	}
	defer indexesFile.Close()
	toCrawlFile, err := os.OpenFile("toCrawl.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Println("Error opening file")
	}
	defer toCrawlFile.Close()
	adjListFile, err := os.OpenFile("adjList.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Println("Error opening file")
	}
	defer adjListFile.Close()
	keywordMapFile, err := os.OpenFile("keywordMap.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Println("Error opening file")
	}
	defer keywordMapFile.Close()
	pointedByFile, err := os.OpenFile("pointedBy.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Println("Error opening file")
	}
	defer pointedByFile.Close()

	indexesFile.Write(indexesMars)
	toCrawlFile.Write(toCrawlMars)
	adjListFile.Write(adjListMars)
	keywordMapFile.Write(keywordMapMars)
	pointedByFile.Write(pointedByMars)

	//store pagerank for Latter use
	rankOfMars, _ := json.Marshal(rankOf)
	rankOfFile, _ := os.OpenFile("rankOf.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	defer rankOfFile.Close()
	rankOfFile.Write(rankOfMars)
	fmt.Println("DONE")
}
