package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/bbalet/stopwords"
)

func main() {
	indexes := make(map[uint64]string)
	toCrawl := make([]uint64, 0)
	adjList := make(map[uint64][]uint64)
	keywordMap := make(map[string][]uint64)
	pointedBy := make(map[uint64][]uint64)
	rankOf := make(map[uint64]float64)

	toCrawl_ := make(map[uint64]bool)
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
	rankOfRead, err := ioutil.ReadFile("rankOf.json")
	if err != nil {
		fmt.Println(err)
	}
	err = json.Unmarshal(rankOfRead, &rankOf)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Rankof Map Loaded")
	{
		fmt.Println("INDEXES ", len(indexes))
		fmt.Println("TOCRAWL ", len(toCrawl))
		fmt.Println("ADJLIST ", len(adjList))
		fmt.Println("KEYWORD ", len(keywordMap))
		fmt.Println("PointedBy", len(pointedBy))
		fmt.Println("Rankof", len(rankOf))
	}

	var query string
	var top int
	flag.StringVar(&query, "q", "github practice go program blog string topic programming set", "Query String")
	flag.IntVar(&top, "top", 10, "Top Query")
	flag.Parse()

	makeQuery(query, top, keywordMap, rankOf, adjList, pointedBy, indexes)

}

func makeQuery(query string, top int, keywordMap map[string][]uint64, rankOf map[uint64]float64, adjList map[uint64][]uint64, pointedBy map[uint64][]uint64, indexes map[uint64]string) {
	//var query = "github practice go program blog string topic programming set"
	//var query = "set"

	//construct subgraph
	pagesWithQueryTerm := make([]uint64, 0)
	reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
	for _, each := range strings.Split(stopwords.CleanString(reg.ReplaceAllString(query, " "), "en", true), " ") {
		if each != "" {
			if _, ok := keywordMap[each]; ok {
				pagesWithQueryTerm = append(pagesWithQueryTerm, keywordMap[each]...)
			}
		}
	}
	//fmt.Println(pagesWithQueryTerm)
	rankOfQPages := make(map[uint64]float64)

	for _, each := range pagesWithQueryTerm {
		rankOfQPages[each] = rankOf[each]
	}

	//sort rankOfQpages and extract top t
	t := 200
	d_HITS := 50
	s := sortbyvalue(rankOfQPages)
	//S := make([]uint64, 0)
	S := make(map[uint64]bool)
	for i := 0; i < t && i < len(s); i++ {
		//S = append(S, s[i].Key)
		S[s[i].Key] = true
		//include all the outlinks
		//S = append(S, adjList[s[i].Key]...)
		for _, eachEle := range adjList[s[i].Key] {
			S[eachEle] = true
		}

		dLimit := 0
		//select first $D_HITS$ inlinks into consideration
		for _, p := range pointedBy[s[i].Key] {
			if dLimit > d_HITS {
				break
			}
			//S = append(S, p)
			S[p] = true
		}
	}

	fmt.Println("OUTPUT", len(S))

	//construct a new graph G with nodes in S and edges connecting only S
	G := make(map[uint64][]uint64)
	for eachNode := range S {
		for _, neighbour := range adjList[eachNode] {
			if _, neighbourInS := S[neighbour]; neighbourInS {
				G[eachNode] = append(G[eachNode], neighbour)
			}
		}
	}

	fmt.Println("O", len(G))

	hubWeight := make(map[uint64]float64)
	authWeight := make(map[uint64]float64)

	//initialize hubweight and authweight to 1
	for eachNode := range G {
		hubWeight[eachNode] = 1.0  //X
		authWeight[eachNode] = 1.0 //Y
	}

	//noOfIter
	k := 10
	for i := 0; i < k; i++ {
		var authSum float64
		var hubSum float64
		for eachNode := range G {
			//I operation
			for _, inlinks := range pointedBy[eachNode] {
				authWeight[eachNode] += hubWeight[inlinks]
			}
			authSum += authWeight[eachNode] * authWeight[eachNode]
			//O operation
			for _, outlinks := range adjList[eachNode] {
				hubWeight[eachNode] += authWeight[outlinks]
			}
			hubSum += hubWeight[eachNode] * hubWeight[eachNode]
		}
		//normalization
		authSumSqrt := math.Sqrt(authSum)
		hubSumSqrt := math.Sqrt(hubSum)
		for eachNode := range G {
			authWeight[eachNode] = authWeight[eachNode] / authSumSqrt
			hubWeight[eachNode] = hubWeight[eachNode] / hubSumSqrt
		}
	}

	count := 0
	fmt.Println("AUTHORITY sCORE")
	finalAuthRank := sortbyvalue(authWeight)
	for _, item := range finalAuthRank {
		if count > top {
			break
		}
		count++
		fmt.Println(indexes[item.Key])
	}
	fmt.Println("HUB SCORE")
	finalHubRank := sortbyvalue(hubWeight)
	count = 0
	for _, item := range finalHubRank {
		if count > top {
			break
		}
		count++
		fmt.Println(indexes[item.Key])
	}
}

func sortbyvalue(tempmap map[uint64]float64) PairList {
	pl := make(PairList, len(tempmap))
	i := 0
	for k, v := range tempmap {
		pl[i] = Pair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}

type Pair struct {
	Key   uint64
	Value float64
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
