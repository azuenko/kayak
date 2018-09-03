package kayak

import "bytes"

func (k *Kayak) receiveRequest(t Tracer, from KAddress, request KRequest) {
	t = t.Fork("receiveRequest")

	// ====== Byzantine behavior if enabled ======
	if k.byzantineFlags&ByzantineFlagIgnoreRequestsFromClientCC01 != 0 {
		if (from == KAddress{0xCC, 0x01}) {
			k.traceF(t.Logf("ByzantineFlagIgnoreRequestsFromClientCC01: ignore"))
			return
		}
	}
	// ======== End of Byzantine behavior ========

	if _, fromServer := k.rkeys[from]; !fromServer && !k.allowExternal {
		k.traceF(t.Logf("rejected as only internal requests allowed"))
		return
	}

	if request.Index > k.round {
		k.traceF(t.Logf("request index is ahead"))
		return
	}

	if request.Index+k.indexTolerance < k.round {
		k.traceF(t.Logf("request index is too behind -- client out of sync or replay attack"))
		return
	}

	buzz := hash(request)
	if _, alreadyReceived := k.jobs[buzz]; alreadyReceived {
		k.traceF(t.Logf("buzz found in jobs, possible replay attack"))
		return
	}

	if _, alreadyProcessed := k.setBuzz[buzz]; alreadyProcessed {
		k.traceF(t.Logf("buzz found in processed requests, possible replay attack"))
		return
	}

	job := KJob{From: from, Timestamp: k.time, Request: request}
	k.traceF(t.Logf("created new %#v", job))
	k.jobs[buzz] = &job
	k.updateEarliestJobTimestamp()
	k.traceF(t.Logf("recorded"))
}

// TODO: 3 functions are essentially the same. With generics it'd be cleaner
func (k *Kayak) receivePropose(t Tracer, from KAddress, propose KPropose) {
	t = t.Fork("receivePropose")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if _, ok := k.proposes[propose.Round]; !ok {
		k.proposes[propose.Round] = make(map[KEpoch]map[KAddress]KPropose)
	}

	if _, ok := k.proposes[propose.Round][propose.Epoch]; !ok {
		k.proposes[propose.Round][propose.Epoch] = make(map[KAddress]KPropose)
	}

	if _, alreadyReceived := k.proposes[propose.Round][propose.Epoch][from]; alreadyReceived {
		k.traceF(t.Logf("rejected as already received"))
		return
	}

	buzz := hash(propose.Job.Request)

	if _, alreadyProcessed := k.setBuzz[buzz]; alreadyProcessed {
		k.traceF(t.Logf("rejected as already processed"))
		return
	}

	k.proposes[propose.Round][propose.Epoch][from] = propose
	k.traceF(t.Logf("recorded"))

}

func (k *Kayak) receiveWrite(t Tracer, from KAddress, write KWrite) {
	t = t.Fork("receiveWrite")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if _, ok := k.writes[write.Round]; !ok {
		k.writes[write.Round] = make(map[KEpoch]map[KHash]map[KAddress]struct{})
	}

	if _, ok := k.writes[write.Round][write.Epoch]; !ok {
		k.writes[write.Round][write.Epoch] = make(map[KHash]map[KAddress]struct{})
	}

	if _, ok := k.writes[write.Round][write.Epoch][write.Hash]; !ok {
		k.writes[write.Round][write.Epoch][write.Hash] = make(map[KAddress]struct{})
	}

	if _, alreadyReceived := k.writes[write.Round][write.Epoch][write.Hash][from]; alreadyReceived {
		k.traceF(t.Logf("rejected as already received"))
		return
	}

	k.writes[write.Round][write.Epoch][write.Hash][from] = struct{}{}
	k.traceF(t.Logf("recorded"))

}

func (k *Kayak) receiveAccept(t Tracer, from KAddress, accept KAccept) {
	t = t.Fork("receiveAccept")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if _, ok := k.accepts[accept.Round]; !ok {
		k.accepts[accept.Round] = make(map[KEpoch]map[KHash]map[KAddress]struct{})
	}

	if _, ok := k.accepts[accept.Round][accept.Epoch]; !ok {
		k.accepts[accept.Round][accept.Epoch] = make(map[KHash]map[KAddress]struct{})
	}

	if _, ok := k.accepts[accept.Round][accept.Epoch][accept.Hash]; !ok {
		k.accepts[accept.Round][accept.Epoch][accept.Hash] = make(map[KAddress]struct{})
	}

	if _, alreadyReceived := k.accepts[accept.Round][accept.Epoch][accept.Hash][from]; alreadyReceived {
		k.traceF(t.Logf("rejected as already received"))
		return
	}

	k.accepts[accept.Round][accept.Epoch][accept.Hash][from] = struct{}{}
	k.traceF(t.Logf("recorded"))

}

func (k *Kayak) maybePropose(t Tracer) bool {
	t = t.Fork("maybePropose")

	if k.key != k.leader() {
		k.traceF(t.Logf("not leader"))
		return false
	}

	if len(k.jobs) == 0 {
		k.traceF(t.Logf("no jobs"))
		return false
	}

	if k.consensusState != ConsensusStateIdle {
		k.traceF(t.Logf("not in expected %#v state", ConsensusStateIdle))
		return false
	}

	// Pick first job
	var job KJob
	for jobHash := range k.jobs {
		job = *k.jobs[jobHash]
		break
	}

	k.traceF(t.Logf("gogo, total jobs: %d, picked %#v", len(k.jobs), job))

	propose := KPropose{Round: k.round, Epoch: k.epoch, Job: job}
	for i, key := range k.keys {

		// ====== Byzantine behavior if enabled ======
		if k.byzantineFlags&ByzantineFlagSendDifferentProposes != 0 {
			t2 := t.Fork("ByzantineFlagSendDifferentProposes")
			if len(k.jobs) < 2 {
				k.traceF(t2.Logf("not enough jobs, returning"))
				return false
			}
			if i%2 == 0 {
				k.traceF(t2.Logf("sending normal"))
				k.sendF(key, propose)
			} else {
				k.traceF(t2.Logf("sending another"))
				var anotherJob KJob
				for jobHash := range k.jobs {
					if k.jobs[jobHash].Request.Nonce == job.Request.Nonce {
						continue
					}
					anotherJob = *k.jobs[jobHash]
					break
				}

				anotherPropose := KPropose{Round: k.round, Epoch: k.epoch, Job: anotherJob}
				k.sendF(key, anotherPropose)
			}
			continue
		}
		// ======== End of Byzantine behavior ========

		k.sendF(key, propose)
	}

	k.traceF(t.Logf("modify consensus state from %#v to %#v", k.consensusState, ConsensusStateIdlePropose))
	k.consensusState = ConsensusStateIdlePropose
	return true

}

func (k *Kayak) maybeWrite(t Tracer) bool {
	t = t.Fork("maybeWrite")

	if k.key == k.leader() {
		if k.consensusState != ConsensusStateIdlePropose {
			k.traceF(t.Logf("leader not in expected %#v state", ConsensusStateIdlePropose))
			return false
		}
	} else {
		if k.consensusState != ConsensusStateIdle {
			k.traceF(t.Logf("follower not in expected %#v state", ConsensusStateIdle))
			return false
		}
	}

	propose, exists := k.proposes[k.round][k.epoch][k.leader()]

	if !exists {
		k.traceF(t.Logf("propose not found"))
		return false
	}

	k.traceF(t.Logf("gogo, pick %#v", propose))

	// TODO: also add into k.jobs?
	k.currentJob = propose.Job
	k.currentBuzz = hash(propose.Job.Request)

	write := KWrite{Round: k.round, Epoch: k.epoch, Hash: k.currentBuzz}

	for _, key := range k.keys {
		k.sendF(key, write)
	}

	k.traceF(t.Logf("modify consensus state from %#v to %#v", k.consensusState, ConsensusStateProposeWrite))
	k.consensusState = ConsensusStateProposeWrite
	return true
}

func (k *Kayak) maybeAccept(t Tracer) bool {
	t = t.Fork("maybeAccept")

	if k.consensusState != ConsensusStateProposeWrite {
		k.traceF(t.Logf("not in expected %#v state", ConsensusStateProposeWrite))
		return false
	}

	if uint(len(k.writes[k.round][k.epoch][k.currentBuzz])) < k.q {
		k.traceF(t.Logf("write quorum (%d/%d) not reached", uint(len(k.writes[k.round][k.epoch][k.currentBuzz])), k.q))
		return false
	}

	k.traceF(t.Logf("gogo, write quorum (%d/%d) reached", uint(len(k.writes[k.round][k.epoch][k.currentBuzz])), k.q))

	accept := KAccept{Round: k.round, Epoch: k.epoch, Hash: k.currentBuzz}
	for _, key := range k.keys {
		k.sendF(key, accept)
	}

	k.traceF(t.Logf("modify consensus state from %#v to %#v", k.consensusState, ConsensusStateWriteAccept))
	k.consensusState = ConsensusStateWriteAccept
	return true
}

func (k *Kayak) maybeDecide(t Tracer) bool {
	t = t.Fork("maybeDecide")

	if k.consensusState != ConsensusStateWriteAccept {
		k.traceF(t.Logf("not in expected %#v state", ConsensusStateWriteAccept))
		return false
	}

	if uint(len(k.accepts[k.round][k.epoch][k.currentBuzz])) < k.q {
		k.traceF(t.Logf("accept quorum (%d/%d) not reached", uint(len(k.accepts[k.round][k.epoch][k.currentBuzz])), k.q))
		return false
	}
	k.traceF(t.Logf("gogo, accept quorum (%d/%d) reached", uint(len(k.accepts[k.round][k.epoch][k.currentBuzz])), k.q))

	response := KResponse{
		Index: k.round,
		Nonce: k.currentJob.Request.Nonce,
	}
	k.sendF(k.currentJob.From, response)

	k.decide(t, k.currentJob.Request.Payload, k.currentBuzz)

	return true
}

func (k *Kayak) decide(t Tracer, data KData, buzz KHash) {
	t = t.Fork("decide")

	k.traceF(t.Logf("with data %#v and buzz %#v", data, buzz))

	k.storage.Append(data)
	k.logData = append(k.logData, data)

	k.logBuzz = append(k.logBuzz, buzz)
	k.setBuzz[buzz] = struct{}{}

	k.logDataHash = append(k.logDataHash, cumDataHash(
		k.logDataHash[len(k.logDataHash)-1],
		k.logData[len(k.logData)-1],
	))

	k.logBuzzHash = append(k.logBuzzHash, cumBuzzHash(
		k.logBuzzHash[len(k.logBuzzHash)-1],
		k.logBuzz[len(k.logData)-1],
	))

	if job, found := k.jobs[buzz]; found {
		k.traceF(t.Logf("remove job as completed %#v", job))
		delete(k.jobs, buzz)
		k.updateEarliestJobTimestamp()
	} else {
		k.traceF(t.Logf("no jobs associated with buzz %#v", buzz))
	}

	k.traceF(t.Logf("increase round from %#v to %#v", k.round, k.round+1))
	k.round++

	if len(data) == len(MagicAddProcess)+AddressSize && bytes.Equal(data[:len(MagicAddProcess)], MagicAddProcess[:]) {
		k.traceF(t.Logf("found add process command"))
		var processKey KAddress
		copy(processKey[:], data[len(MagicAddProcess):])
		k.addProcess(t, processKey)
	}

	if len(data) == len(MagicRemoveProcess)+AddressSize && bytes.Equal(data[:len(MagicRemoveProcess)], MagicRemoveProcess[:]) {
		k.traceF(t.Logf("found remove process command"))
		var processKey KAddress
		copy(processKey[:], data[len(MagicRemoveProcess):])
		k.removeProcess(t, processKey)
	}

	if k.consensusState != ConsensusStateIdle {
		k.traceF(t.Logf("modify consensus state from %#v to %#v", k.consensusState, ConsensusStateIdle))
		k.consensusState = ConsensusStateIdle
	}

	k.rescheduleWhatsup(t)
}
