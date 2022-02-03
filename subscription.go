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

type subscribeRequest struct {
	s    *subscription
	done chan interface{}
}

type unsubscribeRequest struct {
	id   string
	done chan interface{}
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
			var v T
			if u.Value.value != nil {
				v = u.Value.value.(T)
			}
			cs.Updates = append(cs.Updates,
				&Item[*Raw[T]]{
					Path:       u.Path,
					DetailPath: u.DetailPath,
					Value: &Raw[T]{
						serde:  u.Value.serde,
						Bytes:  u.Value.Bytes,
						loaded: u.Value.loaded,
						value:  v,
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

func Unsubscribe(d DB, id string) {
	d.unsubscribe(id)
}

func (d *db) subscribe(s *subscription) {
	sr := &subscribeRequest{
		s:    s,
		done: make(chan interface{}),
	}
	d.subscribes <- sr
	<-sr.done
}

func (d *db) unsubscribe(id string) {
	usr := &unsubscribeRequest{
		id:   id,
		done: make(chan interface{}),
	}
	d.unsubscribes <- usr
	<-usr.done
}

func (d *db) onNewSubscription(sr *subscribeRequest) {
	s := sr.s
	defer close(sr.done)

	for _, path := range s.pathPrefixes {
		d.getOrCreateSubscriptionsByPath(path)[s.id] = s

		if s.receiveInitial {
			items, err := RList[any](
				d,
				&QueryParams{
					path:        fmt.Sprintf("%s%%", path),
					joinDetails: s.joinDetails,
				},
			)
			if err != nil {
				log.Debugf("unable to list initial values for path prefix %v: %v", path, err)
			} else {
				for _, item := range items {
					s.onUpdate(item)
					if s.joinDetails {
						// subscribe for updates to this detail path
						d.getOrCreateDetailSubscriptionsByPath(item.DetailPath)[s.id] = s
					}
				}
				err := s.flush()
				if err != nil {
					log.Debugf("subscriber failed to accept item onUpdate: %v", err)
				}
			}
		}
	}
}

func (d *db) onDeleteSubscription(usr *unsubscribeRequest) {
	id := usr.id
	defer close(usr.done)

	d.subscriptionsByPath.Visit(func(prefix patricia.Prefix, item patricia.Item) error {
		subs := item.(map[string]*subscription)
		delete(subs, id)
		return nil
	})
	d.detailSubscriptionsByPath.Visit(func(prefix patricia.Prefix, item patricia.Item) error {
		subs := item.(map[string]*subscription)
		delete(subs, id)
		return nil
	})
}

func (d *db) onCommit(c *commit) {
	dirty := make(map[string]*subscription, 0)
	d.notifySubscribers(c.t, dirty, &d.subscriptionsByPath, false)
	d.notifySubscribers(c.t, dirty, &d.detailSubscriptionsByPath, true)
	for _, s := range dirty {
		s.flush()
	}
}

func (d *db) notifySubscribers(t *tx, dirty map[string]*subscription, subscriptionsByPath *patricia.Trie, isDetail bool) {
	for path, u := range t.updates {
		log.Debug(path)
		_ = subscriptionsByPath.VisitPrefixes(patricia.Prefix(path), func(prefix patricia.Prefix, item patricia.Item) error {
			for _, s := range item.(map[string]*subscription) {
				if s.joinDetails && !isDetail {
					// assume that this value is an index entry, go ahead and subscribe to the corresponding detail
					_detailPath, err := u.Value.Value()
					if err == nil {
						detailPath, ok := _detailPath.(string)
						if ok {
							d.getOrCreateDetailSubscriptionsByPath(detailPath)[s.id] = s
							detail, err := RGet[any](t, detailPath)
							if err == nil {
								u.Value = detail
								u.DetailPath = detailPath
								s.onUpdate(u)
								dirty[s.id] = s
							} else {
								log.Debugf("Error reading detail: %v", err)
							}
						}
					}
				} else {
					s.onUpdate(u)
					dirty[s.id] = s
				}
			}
			return nil
		})
	}
	for path := range t.deletes {
		_ = subscriptionsByPath.VisitPrefixes(patricia.Prefix(path), func(prefix patricia.Prefix, item patricia.Item) error {
			for _, s := range item.(map[string]*subscription) {
				s.onDelete(path)
				dirty[s.id] = s
			}
			return nil
		})
	}
}

func (d *db) getOrCreateSubscriptionsByPath(path string) map[string]*subscription {
	return doGetOrCreateSubscriptionsByPath(&d.subscriptionsByPath, path)
}

func (d *db) getOrCreateDetailSubscriptionsByPath(path string) map[string]*subscription {
	return doGetOrCreateSubscriptionsByPath(&d.detailSubscriptionsByPath, path)
}

func doGetOrCreateSubscriptionsByPath(subscriptionsByPath *patricia.Trie, path string) map[string]*subscription {
	var subs map[string]*subscription
	_subs := subscriptionsByPath.Get(patricia.Prefix(path))
	if _subs != nil {
		subs = _subs.(map[string]*subscription)
	} else {
		subs = make(map[string]*subscription, 1)
		subscriptionsByPath.Insert(patricia.Prefix(path), subs)
	}
	return subs
}
