package raft

type RPCResponse struct {
	Response interface{}
	Error    error
}

type RPC struct {
	Args   interface{}
	respCh chan RPCResponse
	id     string
}

func (rpc *RPC) Response(resp interface{}, err error) {
	rpc.respCh <- RPCResponse{resp, err}
}
