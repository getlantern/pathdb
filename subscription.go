package pathdb

import (
	"fmt"
	"strings"

	"github.com/tchap/go-patricia/v2/patricia"
)

type Subscription[T any] struct {
	ID             string
	PathPrefixes   []string
	JoinDetails    bool
	ReceiveInitial bool
	OnUpdate       func(*Item[*Raw[T]]) error
	OnDelete       func(path string) error
}

type subscription struct {
	id             string
	pathPrefixes   []string
	joinDetails    bool
	receiveInitial bool
	onUpdate       func(interface{}) error
	onDelete       func(path string) error
}

func Subscribe[T any](d DB, sub *Subscription[T]) error {
	// clean up pathPrefixes in case they include an unnecessary trailing % wildcard
	for i, prefix := range sub.PathPrefixes {
		sub.PathPrefixes[i] = strings.TrimRight(prefix, "%")
	}
	// we have to create a new subscription to adapt the generic onUpdate to a non-generic one because
	// we're not allowed to cast from a func[T] to a func[any]
	s := &subscription{
		id:             sub.ID,
		pathPrefixes:   sub.PathPrefixes,
		joinDetails:    sub.JoinDetails,
		receiveInitial: sub.ReceiveInitial,
		onUpdate: func(v interface{}) error {
			_item := v.(*Item[*Raw[any]])
			item := &Item[*Raw[T]]{
				Path:       _item.Path,
				DetailPath: _item.DetailPath,
				Value: &Raw[T]{
					serde:  _item.Value.serde,
					Bytes:  _item.Value.Bytes,
					loaded: _item.Value.loaded,
					value:  _item.Value.value.(T),
					err:    _item.Value.err,
				},
			}
			return sub.OnUpdate(item)
		},
		onDelete: sub.OnDelete,
	}
	d.subscribe(s)
	return nil
}

func (d *db) subscribe(s *subscription) {
	d.subscribes <- s
}

func (d *db) unsubscribe(id string) {
	d.unsubscribes <- id
}

func (d *db) onNewSubscription(s *subscription) {
	d.subscriptionsByID[s.id] = s
	for _, path := range s.pathPrefixes {
		subs := d.getOrCreateSubscriptionsByPath(path)
		subs[s.id] = s
		d.subscriptionsByPath.Visit(func(prefix patricia.Prefix, item patricia.Item) error {
			log.Debugf("prefix: %v", string(prefix))
			return nil
		})

		if s.receiveInitial {
			initial, err := RList[any](
				d,
				&QueryParams{
					path:        fmt.Sprintf("%s%%", path),
					joinDetails: s.joinDetails, // TODO actually make details subscriptions work
				},
			)
			if err != nil {
				log.Debugf("unable to list initial values for path prefix %v: %v", path, err)
			} else {
				for _, item := range initial {
					err := s.onUpdate(item)
					if err != nil {
						log.Debugf("subscriber failed to accept item onUpdate: %v", err)
					}
				}
			}
		}
	}
}

func (d *db) onDeleteSubscription(id string) {
	s, found := d.subscriptionsByID[id]
	if !found {
		return
	}
	for _, path := range s.pathPrefixes {
		subs := d.getOrCreateSubscriptionsByPath(path)
		delete(subs, s.id)
		if len(subs) == 0 {
			d.subscriptionsByPath.Delete(patricia.Prefix(path))
		}
	}
}

func (d *db) onCommit(c *commit) {
	log.Debugf("Updates: %v", c.t.updates)
	for path, u := range c.t.updates {
		log.Debug(path)
		_ = d.subscriptionsByPath.VisitPrefixes(patricia.Prefix(path), func(prefix patricia.Prefix, item patricia.Item) error {
			for _, s := range item.(map[string]*subscription) {
				err := s.onUpdate(u)
				if err != nil {
					log.Debugf("error on publishing update: %v", err)
				}
			}
			return nil
		})
	}
	for path := range c.t.deletes {
		_ = d.subscriptionsByPath.VisitPrefixes(patricia.Prefix(path), func(prefix patricia.Prefix, item patricia.Item) error {
			for _, s := range item.(map[string]*subscription) {
				err := s.onDelete(path)
				if err != nil {
					log.Debugf("error on publishing delete: %v", err)
				}
			}
			return nil
		})
	}
}

func (d *db) getOrCreateSubscriptionsByPath(path string) map[string]*subscription {
	var subs map[string]*subscription
	_subs := d.subscriptionsByPath.Get(patricia.Prefix(path))
	if _subs != nil {
		subs = _subs.(map[string]*subscription)
	} else {
		subs = make(map[string]*subscription, 1)
		d.subscriptionsByPath.Insert(patricia.Prefix(path), subs)
	}
	return subs
}
