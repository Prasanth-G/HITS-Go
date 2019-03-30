# HITS-Go_implementation

Implementation of Hyperlink Induced Topic Search Algorithm (popularly known as HITS) in GO Language

> HITS is a link analysis algorithm which will rank webpages at Query time. The Rank is based on the given Query, so the results will be more relevent than Content-based algorithms (e.g. Pagerank)

- Run the `Main.go` to crawl the web and to construct Graph out of it.
- Use `AskQuery.go` with the following flags to query
  - `-q` - query
  - e.g : `AskQuery.exe -q=QUERY_STRING`

Reference :
[Kleinberg, Jon M. "Authoritative sources in a hyperlinked environment." Journal of the ACM (JACM) 46.5 (1999): 604-632.](https://www.cs.odu.edu/~jbollen/IR04/readings/kleinberg1998.pdf)
