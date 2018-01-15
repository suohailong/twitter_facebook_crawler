from idatage_export import Espusher



if __name__ == '__main__':
    es = Espusher()
    print('<<----开始推twitter的文章----->>')
    es.run_twitter_pusher()