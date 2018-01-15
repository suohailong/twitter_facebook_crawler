from idatage_export import Espusher



if __name__ == '__main__':
    es = Espusher()
    print('<<----开始推facebook的文章----->>')
    es.run_facebook_pusher()