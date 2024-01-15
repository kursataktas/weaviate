package lockmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func queueLen(q *LockRequest) int {
	var i int
	for q != nil {
		i++
		q = q.Next
	}
	return i
}

func TestLockManagerLock(t *testing.T) {
	t.Run("lock twice", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, S)
		require.NoError(t, err)

		err = m.Lock(getCtx(t), 1, &o, S)
		require.NoError(t, err)

		require.Equal(t, 2, m.locks[o].Queue.Count)
	})

	t.Run("same object: S", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, S)
		require.NoError(t, err)

		err = m.Lock(getCtx(t), 2, &o, S)
		require.NoError(t, err)

		require.Equal(t, 2, queueLen(m.locks[o].Queue))
	})

	t.Run("same object: incompatible lock", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, S)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = m.Lock(ctx, 2, &o, X)
		require.Error(t, err)
		require.Equal(t, 1, queueLen(m.locks[o].Queue))
	})

	t.Run("convert: single lock in queue", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, S)
		require.NoError(t, err)

		err = m.Lock(getCtx(t), 1, &o, X)
		require.NoError(t, err)

		require.Equal(t, 2, m.locks[o].Queue.Count)
	})

	t.Run("convert: multiple locks in queue, compatible", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, IS)
		require.NoError(t, err)

		err = m.Lock(getCtx(t), 2, &o, IS)
		require.NoError(t, err)

		err = m.Lock(getCtx(t), 3, &o, IX)
		require.NoError(t, err)

		// convert tx 1 to IX
		err = m.Lock(getCtx(t), 1, &o, IX)
		require.NoError(t, err)

		require.Equal(t, 3, queueLen(m.locks[o].Queue))
		require.Equal(t, IX, m.locks[o].GroupMode)
	})

	t.Run("convert: multiple locks in queue, incompatible", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, IS)
		require.NoError(t, err)

		err = m.Lock(getCtx(t), 2, &o, IS)
		require.NoError(t, err)

		err = m.Lock(getCtx(t), 3, &o, IX)
		require.NoError(t, err)

		// convert tx 1 to X
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = m.Lock(ctx, 1, &o, X)
		require.Error(t, err)
		require.Equal(t, 3, queueLen(m.locks[o].Queue))
		require.Equal(t, IX, m.locks[o].GroupMode)
	})
}

func TestLockManagerUnlock(t *testing.T) {
	t.Run("empty manager", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		m.Unlock(1, &o)

		require.NotContains(t, m.locks, o)
	})

	t.Run("unknown lock", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, IS)
		require.NoError(t, err)

		m.Unlock(2, &o)

		require.Equal(t, 1, queueLen(m.locks[o].Queue))
	})

	t.Run("unlock", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, IS)
		require.NoError(t, err)

		m.Unlock(1, &o)

		require.NotContains(t, m.locks, o)
	})

	t.Run("unlock should wake up waiting lock", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, S)
		require.NoError(t, err)

		ch := make(chan struct{})
		go func() {
			defer close(ch)

			err := m.Lock(context.TODO(), 2, &o, X)
			require.NoError(t, err)

		}()

		time.Sleep(time.Millisecond)
		m.Unlock(1, &o)

		<-ch
	})

	t.Run("unlock should wake up next waiting lock", func(t *testing.T) {
		m := New()

		o := Object{1, 1}

		err := m.Lock(getCtx(t), 1, &o, S)
		require.NoError(t, err)

		ch1 := make(chan struct{})
		ch2 := make(chan struct{})

		go func() {
			defer close(ch1)

			err := m.Lock(context.TODO(), 2, &o, X)
			require.NoError(t, err)

		}()

		go func() {
			defer close(ch2)

			time.Sleep(time.Millisecond)
			err := m.Lock(context.TODO(), 3, &o, X)
			require.NoError(t, err)

		}()

		time.Sleep(10 * time.Millisecond)
		m.Unlock(1, &o)

		<-ch1

		m.Unlock(2, &o)

		<-ch2
	})
}
