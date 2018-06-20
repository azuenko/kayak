package kayak

func (k *Kayak) receiveWhatsup(t Tracer, from KAddress) {
	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("receiveWhatsup: rejected as not from server"))
		return
	}

	head := KHead{Round: k.round, Epoch: k.epoch}
	k.sendF(from, head)
}

func (k *Kayak) receiveBonjour(t Tracer, from KAddress) {
	tip := KTip{Round: k.round}
	k.sendF(from, tip)
}

func (k *Kayak) receiveHead(t Tracer, from KAddress, head KHead) {
	t = t.Fork("receiveHead")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if head.Round < k.mostRecentRoundKnown {
		k.traceF(t.Logf("rejected as outdated, most recent round known %#v", k.mostRecentRoundKnown))
		return
	}

	if _, ok := k.heads[head.Round]; !ok {
		k.heads[head.Round] = make(map[KEpoch]map[KAddress]struct{})
	}

	if _, ok := k.heads[head.Round][head.Epoch]; !ok {
		k.heads[head.Round][head.Epoch] = make(map[KAddress]struct{})
	}

	if _, alreadyReceived := k.heads[head.Round][head.Epoch][from]; alreadyReceived {
		k.traceF(t.Logf("rejected as already received"))
		return
	}

	k.heads[head.Round][head.Epoch][from] = struct{}{}

	if uint(len(k.heads[head.Round][head.Epoch])) >= k.q {
		k.traceF(t.Logf("head quorum (%d/%d) reached", uint(len(k.heads[head.Round][head.Epoch])), k.q))
		if head.Round >= k.mostRecentRoundKnown {
			if head.Epoch >= k.mostRecentEpochKnown {
				k.traceF(t.Logf("updating most recent known round and epoch from %#v:%#v to %#v:%#v",
					k.mostRecentRoundKnown, k.mostRecentEpochKnown, head.Round, head.Epoch))
				k.mostRecentRoundKnown = head.Round
				k.mostRecentEpochKnown = head.Epoch
			} else {
				k.traceF(t.Logf("head epoch is not the most recent known %#v", k.mostRecentEpochKnown))
			}
		} else {
			k.traceF(t.Logf("head round is not the most recent known %#v", k.mostRecentRoundKnown))
		}
	} else {
		k.traceF(t.Logf("head quorum (%d/%d) not reached", uint(len(k.heads[head.Round][head.Epoch])), k.q))
	}
}

func (k *Kayak) receiveNeed(t Tracer, from KAddress, need KNeed) {
	t = t.Fork("receiveNeed")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if need.First >= need.Last {
		k.traceF(t.Logf("rejected as invalid"))
		return
	}

	if need.Last > k.round {
		k.traceF(t.Logf("rejected as no data can be returned"))
		return
	}

	chunk := KChunk{
		Last: need.Last,
		Data: k.logData[need.First:need.Last],
		Buzz: k.logBuzz[need.First:need.Last],
	}

	for i := 0; i < int(need.Last-need.First); i++ {
		k.traceF(t.Logf("chunk data at %2d: %#v", i, chunk.Data[i]))
		k.traceF(t.Logf("chunk buzz at %2d: %#v", i, chunk.Buzz[i]))
	}
	k.sendF(from, chunk)

}

func (k *Kayak) receiveEnsure(t Tracer, from KAddress, ensure KEnsure) {
	t = t.Fork("receiveEnsure")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if ensure.Last > k.round {
		k.traceF(t.Logf("rejected as no data can be returned"))
		return
	}

	confirm := KConfirm{
		Last:     ensure.Last,
		DataHash: k.logDataHash[ensure.Last],
		BuzzHash: k.logBuzzHash[ensure.Last],
	}
	k.sendF(from, confirm)

}

func (k *Kayak) receiveChunk(t Tracer, from KAddress, chunk KChunk) {
	t = t.Fork("receiveChunk")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if chunk.Last <= k.round {
		k.traceF(t.Logf("rejected as useless"))
		return
	}

	if len(chunk.Data) != len(chunk.Buzz) {
		k.traceF(t.Logf("rejected as invalid -- slice lenghts differ"))
		return
	}

	usefulLen := int(chunk.Last - k.round)

	if usefulLen > len(chunk.Data) {
		k.traceF(t.Logf("rejected as invalid -- not enough data received"))
		return
	}

	k.traceF(t.Logf("received %d useful entries", usefulLen))

	indexFrom := len(chunk.Data) - usefulLen

	logDataHash := cumDataHash(
		k.logDataHash[len(k.logDataHash)-1],
		chunk.Data[indexFrom:]...,
	)

	if _, ok := k.syncData[chunk.Last]; !ok {
		k.syncData[chunk.Last] = make(map[KHash][]KData)
	}

	k.syncData[chunk.Last][logDataHash] = chunk.Data[indexFrom:]

	logBuzzHash := cumBuzzHash(
		k.logBuzzHash[len(k.logBuzzHash)-1],
		chunk.Buzz[indexFrom:]...,
	)

	if _, ok := k.syncBuzz[chunk.Last]; !ok {
		k.syncBuzz[chunk.Last] = make(map[KHash][]KHash)
	}

	k.syncBuzz[chunk.Last][logBuzzHash] = chunk.Buzz[indexFrom:]

	confirm := KConfirm{
		Last:     chunk.Last,
		DataHash: logDataHash,
		BuzzHash: logBuzzHash,
	}
	k.receiveConfirm(t, from, confirm)

}

func (k *Kayak) receiveConfirm(t Tracer, from KAddress, confirm KConfirm) {
	t = t.Fork("receiveConfirm")

	if _, fromServer := k.rkeys[from]; !fromServer {
		k.traceF(t.Logf("rejected as not from server"))
		return
	}

	if confirm.Last <= k.mostRecentRoundToSync {
		k.traceF(t.Logf("rejected as useless -- most recent round to sync is already %#v", k.mostRecentRoundToSync))
		return
	}

	if _, ok := k.confirms[confirm.Last]; !ok {
		k.confirms[confirm.Last] = make(map[KHash]map[KHash]map[KAddress]struct{})
	}
	if _, ok := k.confirms[confirm.Last][confirm.DataHash]; !ok {
		k.confirms[confirm.Last][confirm.DataHash] = make(map[KHash]map[KAddress]struct{})
	}
	if _, ok := k.confirms[confirm.Last][confirm.DataHash][confirm.BuzzHash]; !ok {
		k.confirms[confirm.Last][confirm.DataHash][confirm.BuzzHash] = make(map[KAddress]struct{})
	}

	k.confirms[confirm.Last][confirm.DataHash][confirm.BuzzHash][from] = struct{}{}
	k.traceF(t.Logf("recorded"))

	hasN := uint(len(k.confirms[confirm.Last][confirm.DataHash][confirm.BuzzHash]))
	if hasN >= k.q {
		k.traceF(t.Logf("confirm quorum (%d/%d) reached", hasN, k.q))
		if confirm.Last > k.mostRecentRoundToSync {
			k.traceF(t.Logf("updating most recent round to sync from %#v to %#v", k.mostRecentRoundToSync, confirm.Last))
			k.mostRecentRoundToSync = confirm.Last
			k.traceF(t.Logf("updating most recent hash to sync from %#v to %#v", k.mostRecentHashToSync, confirm.DataHash))
			k.mostRecentHashToSync = confirm.DataHash
			k.traceF(t.Logf("updating most recent buzz to sync from %#v to %#v", k.mostRecentBuzzToSync, confirm.BuzzHash))
			k.mostRecentBuzzToSync = confirm.BuzzHash
		}
	} else {
		k.traceF(t.Logf("confirm quorum (%d/%d) not reached", hasN, k.q))
	}
}

func (k *Kayak) maybeWhatsup(t Tracer) bool {
	t = t.Fork("maybeWhatsup")

	if k.time < k.nextWhatsup {
		k.traceF(t.Logf("not yet: now %#v, next at %#v", k.time, k.nextWhatsup))
		return false
	}

	whatsup := KWhatsup{}
	k.traceF(t.Logf("gogo"))
	for _, key := range k.keys {
		k.sendF(key, whatsup)
	}

	k.rescheduleWhatsup(t)

	return true

}

func (k *Kayak) maybeSync(t Tracer) bool {
	t = t.Fork("maybeSync")

	if k.round >= k.mostRecentRoundKnown {
		k.traceF(t.Logf("no need: at %#v, most recent known %#v", k.round, k.mostRecentRoundKnown))
		return false
	}

	if k.syncSent[k.mostRecentRoundKnown] {
		k.traceF(t.Logf("sync already sent"))
		return false
	}

	k.traceF(t.Logf("gogo, sync from %#v to %#v", k.round, k.mostRecentRoundKnown))

	need := KNeed{Last: k.mostRecentRoundKnown, First: k.round}
	selectedForTransfer := k.getSomeoneElseKey()
	k.sendF(selectedForTransfer, need)

	ensure := KEnsure{Last: k.mostRecentRoundKnown}
	for _, key := range k.keys {
		if key == k.key || key == selectedForTransfer {
			continue
		}
		k.sendF(key, ensure)
	}

	k.syncSent[k.mostRecentRoundKnown] = true

	k.rescheduleWhatsup(t)

	return true

}

func (k *Kayak) maybeUpdate(t Tracer) bool {
	t = t.Fork("maybeUpdate")

	if k.round >= k.mostRecentRoundToSync {
		k.traceF(t.Logf("no need: at %#v, most recent known %#v", k.round, k.mostRecentRoundToSync))
		return false
	}

	missingData, dataFound := k.syncData[k.mostRecentRoundToSync][k.mostRecentHashToSync]
	missingBuzz, buzzFound := k.syncBuzz[k.mostRecentRoundToSync][k.mostRecentBuzzToSync]

	if !dataFound {
		k.traceF(t.Logf("data chunk with hash %#v not found", k.mostRecentHashToSync))
		return false
	}
	if !buzzFound {
		k.traceF(t.Logf("buzz chunk with hash %#v not found", k.mostRecentBuzzToSync))
		return false
	}

	if len(missingData) != len(missingBuzz) {
		k.traceF(t.Logf("slice lengths not equal: %d != %d", len(missingData), len(missingBuzz)))
		return false
	}

	k.traceF(t.Logf("gogo, update from %#v to %#v", k.round, k.mostRecentRoundToSync))

	advanceN := int(k.mostRecentRoundToSync - k.round)

	k.traceF(t.Logf("advancing log to %d entries", advanceN))

	for i := 0; i < advanceN; i++ {
		data := missingData[len(missingData)-advanceN+i]
		buzz := missingBuzz[len(missingBuzz)-advanceN+i]
		k.decide(t, data, buzz)
	}

	if k.epoch < k.mostRecentEpochKnown {
		k.traceF(t.Logf("advancing epoch %#v >> %#v", k.epoch, k.mostRecentEpochKnown))
		k.epoch = k.mostRecentEpochKnown
	} else {
		k.traceF(t.Logf("no need to advance epoch"))
	}

	if k.lcState != LCStateIdle {
		k.traceF(t.Logf("modify leader change state from %#v to %#v", k.lcState, LCStateIdle))
		k.lcState = LCStateIdle
	}

	return true

}

func (k *Kayak) rescheduleWhatsup(t Tracer) {
	if k.nextWhatsup < k.time+k.whatsupT {
		k.traceF(t.Logf("reschedule next Whatsup from %#v to %#v", k.nextWhatsup, k.time+k.whatsupT))
		k.nextWhatsup = k.time + k.whatsupT
	}
}
