package db

type DiscoveryType uint8

const (
	DiscoverFOFN DiscoveryType = iota
	DiscoverFOFNBase64
	DiscoverFOFNQuoted
	DiscoverFODN
	DiscoverFODNBase64
	DiscoverFODNQuoted
	DiscoverFile
	DiscoverDirectory
	DiscoverRemovedFile
	DiscoverRemovedDirectory
)

type Discover struct {
	Path string
	Type DiscoveryType
}

func (d *DB) AddSetDiscovery(set *Set, dr *Discover) error {
	return d.exec(createDiscover, set.id, dr.Path, dr.Type)
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

func (d *DB) DeleteSetDiscovery(set *Set, path string) error {
	return d.exec(deleteDiscover, set.id, path)
}
