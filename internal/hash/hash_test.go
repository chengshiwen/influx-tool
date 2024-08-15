package hash

import (
	"slices"
	"testing"
)

func TestShardTpl(t *testing.T) {
	tests := []struct {
		name   string
		tpl    string
		db     string
		mm     string
		parts  []string
		dbCnt  int
		mmCnt  int
		render string
	}{
		{
			name:   "test1",
			tpl:    "%db,%mm",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"%db", ",", "%mm"},
			dbCnt:  1,
			mmCnt:  1,
			render: "database,measurement",
		},
		{
			name:   "test2",
			tpl:    "shard-%db-%mm",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard-", "%db", "-", "%mm"},
			dbCnt:  1,
			mmCnt:  1,
			render: "shard-database-measurement",
		},
		{
			name:   "test3",
			tpl:    "%db-%mm-key",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"%db", "-", "%mm", "-key"},
			dbCnt:  1,
			mmCnt:  1,
			render: "database-measurement-key",
		},
		{
			name:   "test4",
			tpl:    "shard-%db-%mm-key",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard-", "%db", "-", "%mm", "-key"},
			dbCnt:  1,
			mmCnt:  1,
			render: "shard-database-measurement-key",
		},
		{
			name:   "test5",
			tpl:    "shard-%mm-%db-%mm-%db-key",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard-", "%mm", "-", "%db", "-", "%mm", "-", "%db", "-key"},
			dbCnt:  2,
			mmCnt:  2,
			render: "shard-measurement-database-measurement-database-key",
		},
		{
			name:   "test6",
			tpl:    "%db%mm",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"%db", "%mm"},
			dbCnt:  1,
			mmCnt:  1,
			render: "databasemeasurement",
		},
		{
			name:   "test7",
			tpl:    "shard%db%mm",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard", "%db", "%mm"},
			dbCnt:  1,
			mmCnt:  1,
			render: "sharddatabasemeasurement",
		},
		{
			name:   "test8",
			tpl:    "%db%mmkey",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"%db", "%mm", "key"},
			dbCnt:  1,
			mmCnt:  1,
			render: "databasemeasurementkey",
		},
		{
			name:   "test9",
			tpl:    "shard%db%mmkey",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard", "%db", "%mm", "key"},
			dbCnt:  1,
			mmCnt:  1,
			render: "sharddatabasemeasurementkey",
		},
		{
			name:   "test10",
			tpl:    "shard%mm%db%mm%dbkey",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard", "%mm", "%db", "%mm", "%db", "key"},
			dbCnt:  2,
			mmCnt:  2,
			render: "shardmeasurementdatabasemeasurementdatabasekey",
		},
	}
	for _, tt := range tests {
		st := NewShardTpl(tt.tpl)
		if !slices.Equal(st.parts, tt.parts) || st.freq[ShardKeyVarDb] != tt.dbCnt || st.freq[ShardKeyVarMm] != tt.mmCnt {
			t.Errorf("%v: got %+v, %d, %d, want %+v, %d, %d", tt.name, st.parts, st.freq[ShardKeyVarDb], st.freq[ShardKeyVarMm], tt.parts, tt.dbCnt, tt.mmCnt)
		}
		if render := st.GetKey(tt.db, []byte(tt.mm)); render != tt.render {
			t.Errorf("%v: got %s, want %s", tt.name, render, tt.render)
		}
	}
}

func TestShardTplV2(t *testing.T) {
	tests := []struct {
		name   string
		tpl    string
		org    string
		bk     string
		mm     string
		parts  []string
		orgCnt int
		bkCnt  int
		mmCnt  int
		render string
	}{
		{
			name:   "test1",
			tpl:    "%org,%bk,%mm",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"%org", ",", "%bk", ",", "%mm"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "org,bucket,measurement",
		},
		{
			name:   "test2",
			tpl:    "shard-%org-%bk-%mm",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard-", "%org", "-", "%bk", "-", "%mm"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "shard-org-bucket-measurement",
		},
		{
			name:   "test3",
			tpl:    "%org-%bk-%mm-key",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"%org", "-", "%bk", "-", "%mm", "-key"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "org-bucket-measurement-key",
		},
		{
			name:   "test4",
			tpl:    "shard-%org-%bk-%mm-key",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard-", "%org", "-", "%bk", "-", "%mm", "-key"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "shard-org-bucket-measurement-key",
		},
		{
			name:   "test5",
			tpl:    "shard-%mm-%bk-%org-%mm-%bk-%org-key",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard-", "%mm", "-", "%bk", "-", "%org", "-", "%mm", "-", "%bk", "-", "%org", "-key"},
			orgCnt: 2,
			bkCnt:  2,
			mmCnt:  2,
			render: "shard-measurement-bucket-org-measurement-bucket-org-key",
		},
		{
			name:   "test6",
			tpl:    "%org%bk%mm",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"%org", "%bk", "%mm"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "orgbucketmeasurement",
		},
		{
			name:   "test7",
			tpl:    "shard%org%bk%mm",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard", "%org", "%bk", "%mm"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "shardorgbucketmeasurement",
		},
		{
			name:   "test8",
			tpl:    "%org%bk%mmkey",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"%org", "%bk", "%mm", "key"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "orgbucketmeasurementkey",
		},
		{
			name:   "test9",
			tpl:    "shard%org%bk%mmkey",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard", "%org", "%bk", "%mm", "key"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "shardorgbucketmeasurementkey",
		},
		{
			name:   "test10",
			tpl:    "shard%mm%bk%org%mm%bk%orgkey",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard", "%mm", "%bk", "%org", "%mm", "%bk", "%org", "key"},
			orgCnt: 2,
			bkCnt:  2,
			mmCnt:  2,
			render: "shardmeasurementbucketorgmeasurementbucketorgkey",
		},
	}
	for _, tt := range tests {
		st := NewShardTpl(tt.tpl)
		if !slices.Equal(st.parts, tt.parts) || st.freq[ShardKeyVarOrg] != tt.orgCnt || st.freq[ShardKeyVarBk] != tt.bkCnt || st.freq[ShardKeyVarMm] != tt.mmCnt {
			t.Errorf("%v: got %+v, %d, %d, %d, want %+v, %d, %d, %d", tt.name, st.parts, st.freq[ShardKeyVarOrg], st.freq[ShardKeyVarBk], st.freq[ShardKeyVarMm], tt.parts, tt.orgCnt, tt.bkCnt, tt.mmCnt)
		}
		if render := st.GetKeyV2(tt.org, tt.bk, tt.mm); render != tt.render {
			t.Errorf("%v: got %s, want %s", tt.name, render, tt.render)
		}
	}
}
