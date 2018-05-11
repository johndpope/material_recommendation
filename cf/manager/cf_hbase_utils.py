# encoding=utf8

import happybase


def get_batch_data(data):
    n = len(data)
    batch_size = 10000
    num_of_batch = n / batch_size if n % batch_size == 0 else n / batch_size + 1
    print 'num_of_batch', num_of_batch
    for i in range(num_of_batch):
        start_index = i * batch_size
        end_index = start_index + batch_size
        yield data[start_index: end_index]


def view_actions(row_key_list):
    # row_key_list 太多（比如30w）会超时，因此应该一坨一坨取

    cols = ['info:news_id', 'info:topic_id', 'info:uid']
    # 因为row_key里边的uid可能会出错，因此再取一次info:uid
    topic_actions = []
    news_actions = []

    for batch_row_key_list in get_batch_data(row_key_list):
        print 'batch_row_key_list num', len(batch_row_key_list)
        connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
        table = connection.table("cy_event")
        row = table.rows(batch_row_key_list, columns=cols)

        for key, value in row:
            action_type, ts, _ = key.split('|')
            uid = int(value['info:uid'])
            if 'news' in action_type:
                action_id = value['info:news_id']
                news_actions.append([uid, action_id])
            elif 'topic' in action_type:
                action_id = value['info:topic_id']
                topic_actions.append([uid, action_id])

        connection.close()

    return news_actions, topic_actions
