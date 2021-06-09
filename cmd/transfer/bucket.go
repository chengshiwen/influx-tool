package transfer

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

func makeShardGroupsForDuration(min, max time.Time, sd time.Duration) meta.ShardGroupInfos {
	start := min.Truncate(sd).UTC()
	end := max.Truncate(sd).Add(sd).UTC()

	groups := make(meta.ShardGroupInfos, end.Sub(start)/sd)
	var i uint64
	for start.Before(end) {
		groups[i] = meta.ShardGroupInfo{
			ID:        i,
			StartTime: start,
			EndTime:   start.Add(sd),
		}
		i++
		start = start.Add(sd)
	}
	return groups[:i]
}

// PlanShardGroups creates a new ShardGroup set using a shard group duration of sd, for the time spanning start to end.
func planShardGroups(sourceShards []meta.ShardGroupInfo, sd time.Duration, start, end int64) meta.ShardGroupInfos {
	var target []meta.ShardGroupInfo
	if len(sourceShards) == 0 {
		return target
	}
	min, max := sourceShards[0].StartTime, sourceShards[len(sourceShards)-1].EndTime
	groups := makeShardGroupsForDuration(min, max, sd)
	for i := 0; i < len(groups); i++ {
		g := groups[i]
		if g.StartTime.UnixNano() > end || g.EndTime.UnixNano() <= start {
			continue
		}
		// NOTE: EndTime.Add(-1) matches the Contains interval of [start, end)
		if hasShardsGroupForTimeRange(sourceShards, g.StartTime, g.EndTime.Add(-1)) {
			target = append(target, g)
		}
	}
	return target
}

func hasShardsGroupForTimeRange(groups []meta.ShardGroupInfo, min, max time.Time) bool {
	for _, g := range groups {
		if g.Overlaps(min, max) {
			return true
		}
	}
	return false
}
