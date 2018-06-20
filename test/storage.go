package test

type Storage struct {
	Entries [][]byte
}

func (s *Storage) Append(entry []byte) {
	s.Entries = append(s.Entries, entry)
}
