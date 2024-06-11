package main

import (
	"net/http"
	"testing"
)

var fakeHandlerValue string

func fakeHandler(val string) handler {
	return func(http.ResponseWriter, *http.Request, []params) {
		fakeHandlerValue = val
	}
}

func TestNode_AddAndGet(t *testing.T) {

	tree := &routerNode{}

	routes := [...]string{
		"/",
		"/cmd/:tool/:sub",
		"/cmd/:tool/",
		"/search/",
		"/src/*filepath",
		"/search/:query",
		"/user_:name",
		"/user_:name/about",
		"/files/:dir/*filepath",
		"/doc/",
		"/doc/go_faq.html",
		"/doc/go1.html",
		"/info/:user/public",
		"/info/:user/project/:project",
	}
	for _, route := range routes {
		tree.addRoute(route, fakeHandler(route))
	}

	wantHandlers := []string{
		"/",
		"/cmd/tool1/abc1",
		"/user_kieren",
		"/user_kieren/about",
		"/src/some/file.png",
		"/src/",
		"/files/root/abc",
		"/doc/go1.html",
		"/info/kieren/public",
		"/info/kieren/project/httprouter",
		"/search/q",
	}

	for _, wantHandler := range wantHandlers {

		h, _, tsr := tree.getValue(wantHandler)
		if tsr {
			t.Fatalf("want tsr is false, but got true, fullpath=%s", wantHandler)
		}

		if h == nil {
			t.Fatalf("want handler, but got nil, fullpath=%s", wantHandler)
		}
	}

}

func TestNode_TrailingRedirectSlash(t *testing.T) {

	tree := &routerNode{}

	routes := [...]string{
		"/hi",
		"/b/",
		"/search/:query",
		"/cmd/:tool/",
		"/src/:filepath",
		"/x",
		"/x/y",
		"/y/",
		"/y/z",
		"/0/:id",
		"/0/:id/1",
		"/1/:id/",
		"/1/:id/2",
		"/aa",
		"/a/",
		"/admin",
		"/admin/:category",
		"/admin/:category/:page",
		"/doc",
		"/doc/go_faq.html",
		"/doc/go1.html",
		"/no/a",
		"/no/b",
		"/api/hello/:name",
		"/vendor/:x/",
	}
	for i := range routes {
		route := routes[i]
		tree.addRoute(route, fakeHandler(route))
	}

	// printChildren(tree, "")

	tsrRoutes := [...]string{
		"/hi/",
		"/b",
		"/search/gopher/",
		"/cmd/vet",
		"/src/abc/",
		"/x/",
		"/y",
		"/0/go/",
		"/1/go",
		"/a",
		"/admin/",
		"/admin/config/",
		"/admin/config/permissions/",
		"/doc/",
		"/vendor/x",
	}
	for _, route := range tsrRoutes {
		handler, _, tsr := tree.getValue(route)
		if handler != nil {
			t.Fatalf("non-nil handler for TSR route '%s", route)
		} else if !tsr {
			t.Errorf("expected TSR recommendation for route '%s'", route)
		}
	}

	noTsrRoutes := [...]string{
		"/",
		"/no",
		"/no/",
		"/_",
		"/_/",
		"/api/world/abc",
	}
	for _, route := range noTsrRoutes {
		handler, _, tsr := tree.getValue(route)
		if handler != nil {
			t.Fatalf("non-nil handler for No-TSR route '%s", route)
		} else if tsr {
			t.Errorf("expected no TSR recommendation for route '%s'", route)
		}
	}

}
