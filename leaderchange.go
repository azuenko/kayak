package kayak

func (k *Kayak) receiveSuspect(t Tracer, from KAddress, suspect KSuspect) {
	t = t.Fork("receiveSuspect")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if suspect.Epoch != k.epoch+1 {
		k.traceF(t.Logf("rejected as with incorrect epoch"))
		return
	}

	somethingNew := false
	for lid := range suspect.Loads {
		k.traceF(t.Logf("pick %#v", suspect.Loads[lid]))
		buzz := hash(suspect.Loads[lid].Request)
		if _, loadProcessed := k.setBuzz[buzz]; loadProcessed {
			k.traceF(t.Logf("load already processed"))
		} else {
			k.traceF(t.Logf("load is new"))
			somethingNew = true
			break
		}
	}

	if !somethingNew {
		k.traceF(t.Logf("rejected as nothing new in the Suspect"))
		return
	}

	if _, ok := k.suspects[suspect.Epoch]; !ok {
		k.suspects[suspect.Epoch] = make(map[KAddress]KSuspect)
	}

	if _, alreadyReceived := k.suspects[suspect.Epoch][from]; alreadyReceived {
		k.traceF(t.Logf("rejected as already received"))
		return
	}

	k.suspects[suspect.Epoch][from] = suspect
	k.traceF(t.Logf("recorded"))
}

func (k *Kayak) maybeSuspect(t Tracer) bool {
	t = t.Fork("maybeSuspect")

	if k.lcState != LCStateIdle {
		k.traceF(t.Logf("not in expected %#v state", LCStateIdle))
		return false
	}

	hasTimeoutJobs := len(k.jobs) > 0 && k.earliestJobTimestamp+k.timeout <= k.time
	enoughSuspectsToRunLC := uint(len(k.suspects[k.epoch+1])) >= k.f+1

	if hasTimeoutJobs {
		k.traceF(t.Logf("due to local timeout"))
	}

	if enoughSuspectsToRunLC {
		k.traceF(t.Logf("due to received suspects"))
	}

	if !hasTimeoutJobs && !enoughSuspectsToRunLC {
		k.traceF(t.Logf("neither timeouts nor suspects"))
		return false
	}

	k.traceF(t.Logf("gogo"))

	var loads []KLoad

	if hasTimeoutJobs {
		k.traceF(t.Logf("creating loads from local jobs"))
		for buzz := range k.jobs {
			if k.jobs[buzz].Timestamp+k.timeout <= k.time {
				load := KLoad{
					From:    k.jobs[buzz].From,
					Request: k.jobs[buzz].Request,
				}
				k.traceF(t.Logf("made %#v", load))
				loads = append(loads, load)
			}
		}
		k.traceF(t.Logf("created %d loads from local jobs", len(loads)))
	}

	if enoughSuspectsToRunLC {
		k.traceF(t.Logf("creating loads from suspects"))

		loadsMap := make(map[KHash]KLoad)

		for _, suspect := range k.suspects[k.epoch+1] {
			k.traceF(t.Logf("pick %#v", suspect))
			for _, load := range suspect.Loads {
				k.traceF(t.Logf("pick %#v", load))

				buzz := hash(load.Request)
				if _, alreadyProcessed := k.setBuzz[buzz]; alreadyProcessed {
					k.traceF(t.Logf("request has been already processed, skip"))
					continue
				}
				if _, alreadyInJobs := k.jobs[buzz]; alreadyInJobs {
					k.traceF(t.Logf("request has been already added / not timeout, skip"))
					continue
				}
				loadsMap[buzz] = load
				k.traceF(t.Logf("added"))
			}
		}
		k.traceF(t.Logf("created %d loads from suspects", len(loadsMap)))

		for _, load := range loadsMap {
			loads = append(loads, load)
		}
	}

	suspect := KSuspect{Epoch: k.epoch + 1, Loads: loads}
	for _, key := range k.keys {
		k.sendF(key, suspect)
	}

	k.traceF(t.Logf("modify leader change state from %#v to %#v", k.lcState, LCStateAlert))
	k.lcState = LCStateAlert

	return true

}

func (k *Kayak) maybeLeaderChange(t Tracer) bool {
	t = t.Fork("maybeLeaderChange")

	if k.lcState != LCStateAlert {
		k.traceF(t.Logf("not in expected %#v state", LCStateAlert))
		return false
	}

	if uint(len(k.suspects[k.epoch+1])) < k.q {
		k.traceF(t.Logf("suspect quorum (%d/%d) not reached", uint(len(k.suspects[k.epoch+1])), k.q))
		return false
	}

	k.traceF(t.Logf("gogo, suspect quorum (%d/%d) reached", uint(len(k.suspects[k.epoch+1])), k.q))

	k.traceF(t.Logf("all local %d jobs need to be rescheduled", len(k.jobs)))
	for _, job := range k.jobs {
		job.Timestamp = k.time + k.timeout
	}

	k.traceF(t.Logf("have %d suspects for next epoch %#v", len(k.suspects[k.epoch+1]), k.epoch+1))
	for _, suspect := range k.suspects[k.epoch+1] {
		k.traceF(t.Logf("pick %#v", suspect))
		for _, load := range suspect.Loads {
			k.traceF(t.Logf("pick %#v", load))

			buzz := hash(load.Request)

			if _, alreadyProcessed := k.setBuzz[buzz]; alreadyProcessed {
				k.traceF(t.Logf("request has been already processed, skip"))
				continue
			}

			job := KJob{
				From:      load.From,
				Request:   load.Request,
				Timestamp: k.time + k.timeout,
			}

			k.jobs[buzz] = &job
			k.traceF(t.Logf("added"))
		}
	}
	k.updateEarliestJobTimestamp()

	k.traceF(t.Logf("now has %d jobs", len(k.jobs)))

	k.traceF(t.Logf("old leader %#v", k.leader()))
	k.traceF(t.Logf("increase epoch from %#v to %#v", k.epoch, k.epoch+1))
	k.epoch++

	if k.key == k.leader() {
		k.traceF(t.Logf("I am leader now"))
	} else {
		k.traceF(t.Logf("new leader %#v", k.leader()))
	}

	k.traceF(t.Logf("modify consensus state from %#v to %#v", k.consensusState, ConsensusStateIdle))
	k.consensusState = ConsensusStateIdle

	k.traceF(t.Logf("modify leader change state from %#v to %#v", k.lcState, LCStateIdle))
	k.lcState = LCStateIdle

	return true
}
