package models

type Index struct {
	Name   string   // CLIENT_INDEX
	Column string   // BASE
	Fields []string // [ client_id, fare ]
}

func NewIndex() Index {
	return Index{}
}

func (idx Index) WithName(n string) Index {
	idx.Name = n
	return idx
}

func (idx Index) WithColumn(col string) Index {
	idx.Column = col
	return idx
}

func (idx Index) AppendField(f string) Index {
	idx.Fields = append(idx.Fields, f)
	return idx
}
