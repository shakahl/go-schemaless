package choosers

import (
	"encoding/binary"
	"flag"
	"fmt"
	"strconv"
	"testing"

	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/choosers/chash"
	"github.com/rbastic/go-schemaless/choosers/jump"
	"github.com/rbastic/go-schemaless/choosers/ketama"
	"github.com/rbastic/go-schemaless/choosers/maglev"
	"github.com/rbastic/go-schemaless/choosers/mpc"
	"github.com/rbastic/go-schemaless/choosers/rendezvous"
)

var checkDistribution = flag.Bool("checkDistribution", false, "check the distribution of the different choosers")

func testOneDistribution(t *testing.T, shards int, ch schemaless.Chooser) {

	if !*checkDistribution {
		t.Skip("skipping distribution check")
	}

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	ch.SetBuckets(buckets)

	hits := make(map[string]int)

	k := make([]byte, 8)
	for i := 0; i < shards*(1e4); i++ {
		binary.LittleEndian.PutUint64(k[:], uint64(i))
		hits[ch.Choose(string(k))]++
	}

	// t.Logf("hits=%v", hits)

	var total int
	var peak int

	for _, v := range hits {
		total += v
		if v > peak {
			peak = v
		}
	}

	avg := float64(total) / float64(shards)
	t.Logf("peak=%v avg=%v ratio=%v", peak, avg, float64(peak)/avg)
}

func testDistribution(t *testing.T, newch func() schemaless.Chooser) {
	for _, size := range []int{8, 32, 128, 512, 2048, 8192} {
		t.Run(strconv.Itoa(size), func(t *testing.T) { testOneDistribution(t, size, newch()) })
	}
}

func TestDistributionKetama(t *testing.T) {
	testDistribution(t, func() schemaless.Chooser { return ketama.New() })
}
func TestDistributionCHash(t *testing.T) {
	testDistribution(t, func() schemaless.Chooser { return chash.New() })
}
func TestDistributionMulti(t *testing.T) {
	testDistribution(t, func() schemaless.Chooser { return mpc.New(hash64seed, seeds, 21) })
}
func TestDistributionJump(t *testing.T) {
	testDistribution(t, func() schemaless.Chooser { return jump.New(hash64) })
}
func TestDistributionRendezvous(t *testing.T) {
	testDistribution(t, func() schemaless.Chooser { return rendezvous.New() })
}

func TestDistributionMaglev8(t *testing.T)   { testOneDistribution(t, 8, maglev.New()) }
func TestDistributionMaglev32(t *testing.T)  { testOneDistribution(t, 32, maglev.New()) }
func TestDistributionMaglev128(t *testing.T) { testOneDistribution(t, 128, maglev.New()) }
func TestDistributionMaglev512(t *testing.T) { testOneDistribution(t, 512, maglev.New()) }
