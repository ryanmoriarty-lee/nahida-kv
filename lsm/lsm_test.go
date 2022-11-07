package lsm

import (
	"NahidaKV/utils"
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	opt = &Options{
		WorkDir:             "../work_test",
		SSTableMaxSz:        1024,
		MemTableSize:        1024,
		BlockSize:           1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       3,
	}
)

// TestBase
func TestBase(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	test := func() {
		baseTest(t, lsm, 128)
	}
	runTest(1, test)
}

func TestClose(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	lsm.StartCompacter()
	test := func() {
		baseTest(t, lsm, 128)
		utils.Err(lsm.Close())
		lsm = buildLSM()
		baseTest(t, lsm, 128)
	}
	runTest(1, test)
}

func TestHitStorage(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	e := utils.BuildEntry()
	lsm.Set(e)
	hitMemtable := func() {
		v, err := lsm.memTable.Get(e.Key)
		utils.Err(err)
		utils.CondPanic(!bytes.Equal(v.Value, e.Value), fmt.Errorf("[hitMemtable] !equal(v.Value, e.Value)"))
	}
	hitL0 := func() {
		baseTest(t, lsm, 128)
	}
	hitNotL0 := func() {
		lsm.levels.runOnce(0)
		baseTest(t, lsm, 128)
	}

	hitBloom := func() {
		ee := utils.BuildEntry()
		v, err := lsm.levels.levels[0].tables[0].Serach(ee.Key, &ee.Version)
		utils.CondPanic(v != nil, fmt.Errorf("[hitBloom] v != nil"))
		utils.CondPanic(err != utils.ErrKeyNotFound, fmt.Errorf("[hitBloom] err != utils.ErrKeyNotFound"))
	}

	runTest(1, hitMemtable, hitL0, hitNotL0, hitBloom)
}

func TestPsarameter(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	testNil := func() {
		utils.CondPanic(lsm.Set(nil) != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
		_, err := lsm.Get(nil)
		utils.CondPanic(err != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
	}
	runTest(1, testNil)
}

func TestCompact(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	ok := false
	l0TOLMax := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		lsm.levels.runOnce(1)
		for _, t := range lsm.levels.levels[6].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[l0TOLMax] fid not found"))
	}
	l0ToL0 := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 0)
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTablesL0ToL0(cd)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] lsm.levels.fillTablesL0ToL0(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.levels.levels[0].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] fid not found"))
	}
	nextCompact := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 1)
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.levels.levels[1].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] fid not found"))
	}

	maxToMax := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 6, 6, 6)
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 6, *cd)
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.levels.levels[6].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] fid not found"))
	}
	parallerCompact := func() {
		baseTest(t, lsm, 128)
		cd := buildCompactDef(lsm, 0, 0, 1)
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[parallerCompact] lsm.levels.fillTables(cd) ret == false"))
		go lsm.levels.runCompactDef(0, 0, *cd)
		lsm.levels.runCompactDef(0, 0, *cd)
		isParaller := false
		for _, state := range lsm.levels.compactState.levels {
			if len(state.ranges) != 0 {
				isParaller = true
			}
		}
		utils.CondPanic(!isParaller, fmt.Errorf("[parallerCompact] not is paralle"))
	}
	runTest(1, l0TOLMax, l0ToL0, nextCompact, maxToMax, parallerCompact)
}

func baseTest(t *testing.T, lsm *LSM, n int) {
	e := &utils.Entry{
		Key:       []byte("我爱宵宫"),
		Value:     []byte("我要进米哈游"),
		ExpiresAt: 123,
	}
	//caseList := make([]*utils.Entry, 0)
	//caseList = append(caseList, e)

	lsm.Set(e)
	for i := 1; i < n; i++ {
		ee := utils.BuildEntry()
		lsm.Set(ee)
		// caseList = append(caseList, ee)
	}
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
}

// 驱动模块
func buildLSM() *LSM {
	// init DB Basic Test
	c := make(chan map[uint32]int64, 16)
	opt.DiscardStatsCh = &c
	lsm := NewLSM(opt)
	return lsm
}

func runTest(n int, testFunList ...func()) {
	for _, f := range testFunList {
		for i := 0; i < n; i++ {
			f()
		}
	}
}

func buildCompactDef(lsm *LSM, id, thisLevel, nextLevel int) *compactDef {
	t := targets{
		targetSz:  []int64{0, 10485760, 10485760, 10485760, 10485760, 10485760, 10485760},
		fileSz:    []int64{1024, 2097152, 2097152, 2097152, 2097152, 2097152, 2097152},
		baseLevel: nextLevel,
	}
	def := &compactDef{
		compactorId: id,
		thisLevel:   lsm.levels.levels[thisLevel],
		nextLevel:   lsm.levels.levels[nextLevel],
		t:           t,
		p:           buildCompactionPriority(lsm, thisLevel, t),
	}
	return def
}

func buildCompactionPriority(lsm *LSM, thisLevel int, t targets) compactionPriority {
	return compactionPriority{
		level:    thisLevel,
		score:    8.6,
		adjusted: 860,
		t:        t,
	}
}

func tricky(tables []*table) {
	for _, table := range tables {
		table.ss.Indexs().StaleDataSize = 10 << 20
		t, _ := time.Parse("2006-01-02 15:04:05", "1995-08-10 00:00:00")
		table.ss.SetCreatedAt(&t)
	}
}
func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
