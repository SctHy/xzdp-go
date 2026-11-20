package social

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"

	"xzdp/biz/model/blog"
	"xzdp/biz/pkg/constants"
)

// Publish fixes the delivery mode per post; a subsequent threshold crossing does not hide older posts.
func Publish(ctx context.Context, db *gorm.DB, b *blog.Blog, threshold int64) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Omit("CreateTime", "UpdateTime").Create(b).Error; err != nil {
			return err
		}
		return tx.Exec("INSERT INTO feed_publication(blog_id,author_id,published,pull) SELECT ?,?,UNIX_TIMESTAMP(),COUNT(*)>=? FROM tb_follow WHERE follow_user_id=?", b.ID, b.UserId, threshold, b.UserId).Error
	})
}
func (s Store) deliver(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, "SELECT blog_id,author_id,published FROM feed_publication WHERE pull=0 AND delivered=0 ORDER BY blog_id LIMIT 50")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, author, stamp int64
		if err = rows.Scan(&id, &author, &stamp); err != nil {
			return err
		}
		fans, err := s.DB.QueryContext(ctx, "SELECT DISTINCT user_id FROM tb_follow WHERE follow_user_id=?", author)
		if err != nil {
			return err
		}
		p := s.Redis.Pipeline()
		for fans.Next() {
			var fan int64
			if err = fans.Scan(&fan); err != nil {
				fans.Close()
				return err
			}
			p.ZAdd(ctx, constants.FEED_KEY+strconv.FormatInt(fan, 10), &redis.Z{Score: float64(stamp), Member: id})
		}
		err = fans.Err()
		fans.Close()
		if err != nil {
			return err
		}
		if _, err = p.Exec(ctx); err != nil {
			return err
		}
		if _, err = s.DB.ExecContext(ctx, "UPDATE feed_publication SET delivered=1 WHERE blog_id=?", id); err != nil {
			return err
		}
	}
	return rows.Err()
}

type entry struct {
	ID    string
	Stamp int64
}

func Timeline(ctx context.Context, db *gorm.DB, r *redis.Client, user int64, q *blog.FollowBlogReq) (*blog.FollowBlogRresp, error) {
	maxTime := time.Now().Unix()
	var err error
	if q.LastId != "" && q.LastId != "+inf" {
		maxTime, err = strconv.ParseInt(q.LastId, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	if q.Offset < 0 || q.Offset > 100000 {
		return nil, fmt.Errorf("invalid feed offset")
	}
	result := &blog.FollowBlogRresp{List: []*blog.Blog{}, MinTime: "0"}
	offset := q.Offset
	for len(result.List) < constants.MAX_PAGE_SIZE {
		remaining := constants.MAX_PAGE_SIZE - len(result.List)
		page, err := readPage(ctx, db, r, user, maxTime, offset, remaining)
		if err != nil {
			return nil, err
		}
		if page.MinTime == "0" {
			break
		}
		result.List = append(result.List, page.List...)
		result.Offset, result.MinTime = page.Offset, page.MinTime
		maxTime, _ = strconv.ParseInt(page.MinTime, 10, 64)
		offset = page.Offset
	}
	return result, nil
}

func readPage(ctx context.Context, db *gorm.DB, r *redis.Client, user, maxTime, offset int64, size int) (*blog.FollowBlogRresp, error) {
	limit := offset + int64(size)
	entries := make(map[string]int64)
	pushed, err := r.ZRevRangeByScoreWithScores(ctx, constants.FEED_KEY+strconv.FormatInt(user, 10), &redis.ZRangeBy{Min: "0", Max: strconv.FormatInt(maxTime, 10), Count: limit}).Result()
	if err != nil {
		return nil, err
	}
	for _, z := range pushed {
		entries[z.Member.(string)] = int64(z.Score)
	}
	var pulled []entry
	err = db.WithContext(ctx).Raw(`SELECT CAST(p.blog_id AS CHAR) AS id,p.published AS stamp FROM feed_publication p
 WHERE (p.pull OR NOT p.delivered) AND p.published<=? AND EXISTS(SELECT 1 FROM tb_follow f WHERE f.user_id=? AND f.follow_user_id=p.author_id)
 ORDER BY p.published DESC,CAST(p.blog_id AS CHAR) DESC LIMIT ?`, maxTime, user, limit).Scan(&pulled).Error
	if err != nil {
		return nil, err
	}
	for _, p := range pulled {
		entries[p.ID] = p.Stamp
	}
	sorted := make([]entry, 0, len(entries))
	for id, stamp := range entries {
		sorted = append(sorted, entry{id, stamp})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Stamp == sorted[j].Stamp {
			return sorted[i].ID > sorted[j].ID
		}
		return sorted[i].Stamp > sorted[j].Stamp
	})
	res := &blog.FollowBlogRresp{List: []*blog.Blog{}, MinTime: "0"}
	ids := make([]string, 0, constants.MAX_PAGE_SIZE)
	skip := offset
	for _, e := range sorted {
		if e.Stamp == maxTime && skip > 0 {
			skip--
			continue
		}
		ids = append(ids, e.ID)
		t := strconv.FormatInt(e.Stamp, 10)
		if res.MinTime != t {
			res.MinTime = t
			res.Offset = 0
			if e.Stamp == maxTime {
				res.Offset = offset
			}
		}
		res.Offset++
		if len(ids) == size {
			break
		}
	}
	if len(ids) == 0 {
		return res, nil
	}
	var items []*blog.Blog
	if err = db.WithContext(ctx).Where("id IN ? AND EXISTS(SELECT 1 FROM tb_follow f WHERE f.user_id=? AND f.follow_user_id=tb_blog.user_id)", ids, user).Find(&items).Error; err != nil {
		return nil, err
	}
	byID := map[string]*blog.Blog{}
	for _, b := range items {
		byID[strconv.FormatInt(b.ID, 10)] = b
	}
	for _, id := range ids {
		if b := byID[id]; b != nil {
			res.List = append(res.List, b)
		}
	}
	return res, nil
}
