exception RpcException {
  1: i32  code,
  2: string msg
}

service MaterialRecommendationService{
    // input_arg: json.dumps([{'user_id':11111,'timestamps':1500000000,'resource_type':'news'}])
    // return : json.dumps({'output':[{'id':222222,'title':'hhhhhh'}]})
    string  article_recommend(1: string input_arg)   throws (1: RpcException re),

    // input_arg: json.dumps({'user_id':11111,'num':4})
    // return : json.dumps({'output':[id1,id2,id3,...])
    string recommend_topn_topic(1: string input_arg)   throws (1: RpcException re),

    // input_arg: json.dumps({'user_id':11111})
    // return : json.dumps({'output':{'words’:[w1,w2,w3,w4,w5,w6],'plan':[{'url':url1,'name':name1},{'url':url2,'name':name2}]})
    string recommend_tags(1: string input_arg)   throws (1: RpcException re),

    // input_arg: json.dumps({'user_id':11111,'timestamp':17621321,'num':6,'problem_id':222})
    // return: json.dumps({'output':[{'id':111,'type':'topic','title':'xxxx'},{'id':222,'type':'news','title':'yyy'}...])
    string recommend_list(1: string input_arg)   throws (1: RpcException re),

    // input_arg: json.dumps({'user_id':11111,'top_n':4})
    // return: json.dumps({'ids':[id1,id2,...]})
    string recommend_plan(1: string input_arg)   throws (1: RpcException re),

    // input_arg: json.dumps({'user_id':11111,'top_n':2})
    // return: json.dumps({'ids':[id1,id2,...]})
    string recommend_news(1: string input_arg)   throws (1: RpcException re),
}
