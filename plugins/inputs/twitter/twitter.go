package twitter

// simple.go

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"github.com/ChimeraCoder/anaconda"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

// Twitter ... all things needed for interaction with Twitter
type Twitter struct {
	ConsumerKey       string
	ConsumerSecret    string
	AccessToken       string
	AccessTokenSecret string
	KeywordsToTrack   string

	api    *anaconda.TwitterApi
	wg     *sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// Description ... required by plugin input interface
func (t *Twitter) Description() string {
	return "plugin for ingesting events from twitter"
}

// SampleConfig ... required by plugin input interface
func (t *Twitter) SampleConfig() string {
	return `
  ## These values are obtained from apps.twitter.com
  consumer_key = ""
  consumer_secret = ""
  access_token = ""
  access_token_secret = ""
  keywords_to_track = ""
`
}

// Gather ... required by plugin input interface
func (t *Twitter) Gather(acc telegraf.Accumulator) error {
	return nil
}

// Start ... called once on start up of this plugin
func (t *Twitter) Start(acc telegraf.Accumulator) error {
	anaconda.SetConsumerKey(t.ConsumerKey)
	anaconda.SetConsumerSecret(t.ConsumerSecret)
	t.api = anaconda.NewTwitterApi(t.AccessToken, t.AccessTokenSecret)

	// Store the cancel func so we can stop this plugin
	t.ctx, t.cancel = context.WithCancel(context.Background())

	t.wg = &sync.WaitGroup{}
	t.wg.Add(1)
	go t.fetchTweets(acc)

	return nil
}

// Stop ... called once when plugin is stopped
func (t *Twitter) Stop() {
	t.cancel()
	t.wg.Wait()
	return
}

func (t *Twitter) fetchTweets(acc telegraf.Accumulator) {
	defer t.wg.Done()

	keywordList := strings.Split(t.KeywordsToTrack, ",")
	v := url.Values{}
	v.Set("track", t.KeywordsToTrack)
	stream := t.api.PublicStreamFilter(v)
	for item := range stream.C {
		select {
		case <-t.ctx.Done():
			stream.Stop()
			return
		default:
			switch tweet := item.(type) {
			case anaconda.Tweet:
				fields := make(map[string]interface{})
				tags := make(map[string]string)
				if tweet.Lang != "" {
					tags["lang"] = tweet.Lang
				}
				fields["retweet_count"] = tweet.RetweetCount
				fields["tweet_id"] = tweet.IdStr
				fields["follower_count"] = tweet.User.FollowersCount
				fields["screen_name"] = tweet.User.ScreenName
				fields["friends_count"] = tweet.User.FriendsCount
				fields["favorites_count"] = tweet.User.FavouritesCount
				fields["user_verified"] = tweet.User.Verified
				fields["raw"] = tweet.Text
				time, err := tweet.CreatedAtTime()
				if err != nil {
					acc.AddError(err)
					continue
				}
				for _, keyword := range keywordList {
					if strings.Contains(strings.ToLower(tweet.Text), strings.ToLower(keyword)) {
						tags["keyword"] = strings.ToLower(keyword)
						acc.AddFields("tweets", fields, tags, time)
					}
				}

			}
		}
	}
}

func init() {
	inputs.Add("twitter", func() telegraf.Input { return &Twitter{} })
}
