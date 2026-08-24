package catalog

import "testing"

func BenchmarkCatalogQuery(b *testing.B) {
	catalog, err := New(DefaultTemplates())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if items := catalog.Query("document"); len(items) != 3 {
			b.Fatalf("items = %d, want 3", len(items))
		}
	}
}
