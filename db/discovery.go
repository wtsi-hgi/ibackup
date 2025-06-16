package db

type Discover struct {
	Path string
	Type uint8
}

func (d *DB) AddSetDiscovery(set *Set, dr *Discover) error {
	return d.exec(createDiscover, dr.Path, dr.Type, set.id)
}

func (d *DBRO) GetSetDiscovery(set *Set) *IterErr[*Discover] {
	return iterRows(d, scanDiscover, getSetDiscovery, set.id)
}

func scanDiscover(scanner scanner) (*Discover, error) {
	d := new(Discover)

	if err := scanner.Scan(&d.Path, &d.Type); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *DB) DeleteDiscover(set *Set, path string) error {
	return d.exec(deleteDiscover, set.id, path)
}
