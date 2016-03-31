package treedb

type session struct {
	seq       uint32
	blockings map[uint32]chan interface{}
}

func newSession(cap int) *session {
	return &session{blockings: make(map[uint32]chan interface{}, cap)}
}

func (s *session) Block(reply chan interface{}) uint32 {
	s.seq++
	s.blockings[s.seq] = reply
	return s.seq
}

func (s *session) Unblock(seq uint32) chan interface{} {
	if reply, ok := s.blockings[seq]; ok {
		delete(s.blockings, seq)
		return reply
	}
	return nil
}

func (s *session) Reply(seq uint32, result interface{}) bool {
	if reply, ok := s.blockings[seq]; ok {
		delete(s.blockings, seq)
		reply <- result
		return true
	}
	return false
}

func (s *session) Len() int {
	return len(s.blockings)
}

func (s *session) ReplyAll(result interface{}) {
	n := len(s.blockings)
	switch {
	case n == 0:
	case n <= 128:
		for seq, reply := range s.blockings {
			reply <- result
			delete(s.blockings, seq)
		}
	default:
		for _, reply := range s.blockings {
			reply <- result
		}
		s.blockings = make(map[uint32]chan interface{})
	}
}
