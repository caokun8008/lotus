package sealing

import (
	"context"

	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"

	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/go-state-types/abi"
)

type PreCommitPolicy interface {
	Expiration(ctx context.Context, ps ...Piece) (abi.ChainEpoch, error)
}

type Chain interface {
	ChainHead(ctx context.Context) (TipSetToken, abi.ChainEpoch, error)
	StateNetworkVersion(ctx context.Context, tok TipSetToken) (network.Version, error)
}

// BasicPreCommitPolicy satisfies PreCommitPolicy. It has two modes:
//
// Mode 1: The sector contains a non-zero quantity of pieces with deal info 模式1：该部门包含非零数量的交易信息
// Mode 2: The sector contains no pieces with deal info 模式2：该部门不包含交易信息
//
// The BasicPreCommitPolicy#Expiration method is given a slice of the pieces
// which the miner has encoded into the sector, and from that slice picks either
// the first or second mode.为BasicPreCommitPolicy＃Expiration方法提供了矿工已编码到扇区中的片段的一部分，然后从该片段中选择第一或第二模式。
//
// If we're in Mode 1: The pre-commit expiration epoch will be the maximum
// deal end epoch of a piece in the sector. 如果我们处于模式1中，则预提交到期时间段将是该扇区中一块交易的最大交易结束时间段。
//
// If we're in Mode 2: The pre-commit expiration epoch will be set to the
// current epoch + the provided default duration. 如果我们处于模式2：预提交的到期时间将设置为当前时间+提供的默认持续时间。
type BasicPreCommitPolicy struct {
	api Chain

	provingBoundary abi.ChainEpoch  //证明边界
	duration        abi.ChainEpoch
}

// NewBasicPreCommitPolicy produces a BasicPreCommitPolicy
func NewBasicPreCommitPolicy(api Chain, duration abi.ChainEpoch, provingBoundary abi.ChainEpoch) BasicPreCommitPolicy {
	return BasicPreCommitPolicy{
		api:             api,
		provingBoundary: provingBoundary,
		duration:        duration,
	}
}

// Expiration produces the pre-commit sector expiration epoch for an encoded
// replica containing the provided enumeration of pieces and deals.为包含提供的枚举和交易枚举的编码副本生成预提交扇区到期时间。
func (p *BasicPreCommitPolicy) Expiration(ctx context.Context, ps ...Piece) (abi.ChainEpoch, error) {
	_, epoch, err := p.api.ChainHead(ctx)
	if err != nil {
		return 0, err
	}

	var end *abi.ChainEpoch

	for _, p := range ps {
		if p.DealInfo == nil {
			continue
		}

		if p.DealInfo.DealSchedule.EndEpoch < epoch {
			log.Warnf("piece schedule %+v ended before current epoch %d", p, epoch)
			continue
		}

		if end == nil || *end < p.DealInfo.DealSchedule.EndEpoch {
			tmp := p.DealInfo.DealSchedule.EndEpoch
			end = &tmp
		}
	}

	if end == nil {
		tmp := epoch + p.duration
		end = &tmp
	}

	*end += miner.WPoStProvingPeriod - (*end % miner.WPoStProvingPeriod) + p.provingBoundary - 1

	return *end, nil
}
