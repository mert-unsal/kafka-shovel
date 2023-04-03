package util

type set struct {
	item map[string]bool
}

func NewSet() *set {
	return &set{make(map[string]bool)}
}

func (s *set) AddCollection(values []string) {
	for _, value := range values {
		s.add(value)
	}
}

func (s *set) add(value string) {
	s.item[value] = true
}

func (s set) Contains(value string) bool {
	return s.item[value]
}
