package main

import "net/http"

const (
	nStatic = iota
	nRoot
	nParams
	nCatchAll
)

type params struct {
	param string
	value string
}

type handler func(w http.ResponseWriter, r *http.Request, params []params)

type routerNode struct {
	path      string
	indices   string
	nType     int
	wildChild bool
	children  []*routerNode
	handler   handler
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func commonPrefix(a, b string) string {
	end := min(len(a), len(b))
	idx := 0
	for ; idx < end; idx++ {
		if a[idx] != b[idx] {
			break
		}
	}
	return a[:idx]
}

func findWildcard(path string) (wilcard string, i int, valid bool) {
	// Find start
	for start, c := range []byte(path) {
		// A wildcard starts with ':' (param) or '*' (catch-all)
		if c != ':' && c != '*' {
			continue
		}

		// Find end and check for invalid characters
		valid = true
		for end, c := range []byte(path[start+1:]) {
			switch c {
			case '/':
				return path[start : start+1+end], start, valid
			case ':', '*':
				valid = false
			}
		}
		return path[start:], start, valid
	}
	return "", -1, false
}

func (n *routerNode) insertChild(path string, h handler) {

	for {

		wilcard, start, valid := findWildcard(path)

		if start == -1 {
			break
		}

		if !valid {
			panic("invalid wildcard path, per segment path allow one params")
		}

		if len(wilcard) < 2 {
			panic("invalid wildcard pattern path")
		}

		if len(n.children) > 0 {
			panic("only one params pattern path allow in per segment")
		}

		if start > 0 {
			n.path = path[:start]
			n.wildChild = true
			n.nType = nStatic
			n.indices = ""
		}

		child := &routerNode{
			path: wilcard,
		}

		switch wilcard[0] {
		case '*':
			child.nType = nCatchAll
		case ':':
			child.nType = nParams
		default:
			panic("unknown params pattern...")
		}

		n.children = append(n.children, child)
		n = child

		path = path[start+len(wilcard):]

		if len(path) == 0 {
			break
		}

		if path[0] != '*' && path[0] != ':' {
			child := &routerNode{}
			n.children = append(n.children, child)
			n.indices += string(path[0])
			n = child
		}

	}

	n.path = path
	n.handler = h
}

func (n *routerNode) addRoute(fullPath string, h handler) {

	path := fullPath

	if len(n.path) == 0 && len(n.indices) == 0 {
		n.insertChild(path, h)
		n.nType = nRoot
		return
	}

walk:
	for {

		prefix := commonPrefix(n.path, path)

		if len(prefix) < len(n.path) {
			child := &routerNode{
				path:      n.path[len(prefix):],
				indices:   n.indices,
				nType:     nStatic,
				wildChild: n.wildChild,
				children:  n.children,
				handler:   n.handler,
			}

			n.path = n.path[:len(prefix)]
			n.indices = string(child.path[0])
			n.wildChild = false
			n.handler = nil
			n.children = []*routerNode{child}
		}

		if len(prefix) < len(path) {

			path = path[len(prefix):]
			if n.wildChild {
				n = n.children[0]
				if len(path) >= len(n.path) && path[:len(n.path)] == n.path && n.nType != nCatchAll {
					continue walk
				}
				panic("params segment path only allow one path, duplicate path " + fullPath)
			}
			idxc := path[0]
			for ix, c := range n.indices {
				if byte(c) == idxc {
					n = n.children[ix]
					continue walk
				}
			}

			if idxc != '*' && idxc != ':' {
				child := &routerNode{}
				n.indices += string(idxc)
				n.children = append(n.children, child)
				n = child
			}

			n.insertChild(path, h)

			return
		}

		if h != nil {
			n.handler = h
			return
		}

		panic("invalid nil handle for path " + fullPath)
	}

}

func (n *routerNode) getValue(fullPath string) (h handler, ps []params, tsr bool) {

walk:

	path := fullPath
	for {

		if len(path) > len(n.path) {

			idxc := path[0]
			if path[:len(n.path)] == n.path {
				if !n.wildChild {
					for ix, c := range n.indices {
						if byte(c) == idxc {
							path = path[len(n.path):]
							n = n.children[ix]
							continue walk
						}
					}
					tsr = len(path) == len(n.path)+1 && idxc == '/' && n.handler != nil
					return
				}

				path = path[len(n.path):]
				n = n.children[0]

				switch n.nType {
				case nCatchAll:
					ps = append(ps, params{
						param: n.path,
						value: path,
					})

					h = n.handler
					return
				case nParams:
					// wildcard scene
					// search the trailing slash.
					end := 0
					for ; end < len(path); end++ {
						if path[end] == '/' {
							break
						}
					}
					ps = append(ps, params{
						param: n.path,
						value: path[:end],
					})

					if end < len(path) {
						for ix, c := range n.indices {
							if byte(c) == path[end] {
								n = n.children[ix]
								path = path[end:]
								continue walk
							}
						}

						tsr = end+1 == len(path) && n.handler != nil
						return
					}

					if h = n.handler; h != nil {
						return
					}

					tsr = len(n.children) == 1 && n.children[0].path == "/" && n.children[0].handler != nil
					return

				default:
					panic("unsupport params pattern type path segment.")
				}
			}

		} else if path == n.path {

			if h = n.handler; h != nil {
				return
			}

			if path == "/" && n.wildChild && n.nType != nRoot {
				tsr = true
				return
			}

			if path == "/" && n.nType == nStatic {
				tsr = true
				return
			}

			for ix, c := range n.indices {

				n = n.children[ix]
				if byte(c) == '/' {
					if (len(n.path) == 1 && n.handler != nil) ||
						(n.wildChild && n.children[0].nType == nCatchAll && n.children[0].handler != nil) {
						tsr = true
						return
					}
				}
			}

			return
		}

		tsr = path == "/" || (len(n.path) == len(path)+1 && n.path[len(path)] == '/' &&
			n.path[:len(path)] == path && n.handler != nil)

		return

	}

}
