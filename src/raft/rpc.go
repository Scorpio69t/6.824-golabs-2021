package raft

type RPCResponse struct {
	Response interface{}
	Error    error
}

type RPC struct {
	Args   interface{}
	respCh chan RPCResponse
}

func (rpc *RPC) Response(resp interface{}, err error) {
	rpc.respCh <- RPCResponse{resp, err}
}

func MakeRPC(args interface{}, respCh chan RPCResponse) RPC {
	return RPC{
		args, respCh,
	}
}
