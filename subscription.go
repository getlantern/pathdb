package pathdb

import (
	"fmt"
	"strings"

	"github.com/tchap/go-patricia/v2/patricia"
)

type ChangeSet[T any] struct {
	Updates []*Item[*Raw[T]]
	Deletes []string
}

type Subscription[T any] struct {
	ID             string
	PathPrefixes   []string
	JoinDetails    bool
	ReceiveInitial bool
	OnUpdate       func(*ChangeSet[T]) error
}

type subscription struct {
	id             string
	pathPrefixes   []string
	joinDetails    bool
	receiveInitial bool
	onUpdate       func(*Item[*Raw[any]])
	onDelete       func(string)
	flush          func() error
}

func Subscribe[T any](d DB, sub *Subscription[T]) error {
	// clean up pathPrefixes in case they include an unnecessary trailing % wildcard
	for i, prefix := range sub.PathPrefixes {
		sub.PathPrefixes[i] = strings.TrimRight(prefix, "%")
	}

	// we have to create a new subscription to adapt the generic onUpdate to a non-generic one because
	// we're not allowed to cast from a func[T] to a func[any]
	var cs *ChangeSet[T]
	initChangeset := func() {
		cs = &ChangeSet[T]{}
	}
	initChangeset()

	s := &subscription{
		id:             sub.ID,
		pathPrefixes:   sub.PathPrefixes,
		joinDetails:    sub.JoinDetails,
		receiveInitial: sub.ReceiveInitial,
		onUpdate: func(u *Item[*Raw[any]]) {
			cs.Updates = append(cs.Updates,
				&Item[*Raw[T]]{
					Path:       u.Path,
					DetailPath: u.DetailPath,
					Value: &Raw[T]{
						serde:  u.Value.serde,
						Bytes:  u.Value.Bytes,
						loaded: u.Value.loaded,
						value:  u.Value.value.(T),
						err:    u.Value.err,
					},
				})
		},
		onDelete: func(p string) {
			cs.Deletes = append(cs.Deletes, p)
		},
		flush: func() error {
			err := sub.OnUpdate(cs)
			initChangeset()
			return err
		},
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
			items, err := RList[any](
				d,
				&QueryParams{
					path:        fmt.Sprintf("%s%%", path),
					joinDetails: s.joinDetails, // TODO actually make details subscriptions work
				},
			)
			if err != nil {
				log.Debugf("unable to list initial values for path prefix %v: %v", path, err)
			} else {
				for _, item := range items {
					s.onUpdate(item)
				}
				err := s.flush()
				if err != nil {
					log.Debugf("subscriber failed to accept item onUpdate: %v", err)
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
	dirty := make(map[string]*subscription, 0)
	for path, u := range c.t.updates {
		log.Debug(path)
		_ = d.subscriptionsByPath.VisitPrefixes(patricia.Prefix(path), func(prefix patricia.Prefix, item patricia.Item) error {
			for _, s := range item.(map[string]*subscription) {
				s.onUpdate(u)
				dirty[s.id] = s
			}
			return nil
		})
	}
	for path := range c.t.deletes {
		_ = d.subscriptionsByPath.VisitPrefixes(patricia.Prefix(path), func(prefix patricia.Prefix, item patricia.Item) error {
			for _, s := range item.(map[string]*subscription) {
				s.onDelete(path)
				dirty[s.id] = s
			}
			return nil
		})
	}
	for _, s := range dirty {
		s.flush()
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
