
# frozen_string_literal: true

NoSE::Workload.new do
  Model 'rubis'

  #RUBISのクエリの中からORDER BYを削除した
  # Define queries and their relative weights, weights taken from below
  # http://rubis.ow2.org/results/SB-BMP/Bidding/JBoss-SB-BMP-Bi-1500/perf.html#run_stat
  # http://rubis.ow2.org/results/SB-BMP/Browsing/JBoss-SB-BMP-Br-1500/perf.html#run_stat
  DefaultMix :bidding

  Group 'BrowseCategories', browsing: 4.44,
        bidding: 7.65,
        write_medium: 7.65,
        write_hard: 7.65,
        write_heavy: 7.65,
        write_extreme: 7.65 do
    Q 'SELECT users.nickname, users.password FROM users WHERE users.id = ? -- 1'
    Q 'SELECT users.nickname, users.password FROM users WHERE users.rating = ? -- 1_secondary'
    # XXX Must have at least one equality predicate
    Q 'SELECT categories.id, categories.name FROM categories WHERE categories.dummy = 1 -- 2'
    #Q 'SELECT categories.id, categories.name FROM categories WHERE categories.id= 1 -- 2_secondary'
  end

  Group 'ViewBidHistory', browsing: 2.38,
        bidding: 1.54,
        write_medium: 1.54,
        write_hard: 1.54,
        write_heavy: 1.54,
        write_extreme: 1.54 do
    Q 'SELECT items.name FROM items WHERE items.id = ? -- 3'
    Q 'SELECT items.name FROM items WHERE items.quantity = ? -- 3_secondary'
    Q 'SELECT users.id, users.nickname, bids.id, item.id, bids.qty, bids.bid, bids.date FROM users.bids.item WHERE item.id = ? -- 4'
    Q 'SELECT users.id, users.nickname, bids.id, item.id, bids.qty, bids.bid, bids.date FROM users.bids.item WHERE item.quantity = ? -- 4_secondary'
  end

  Group 'ViewItem', browsing: 22.95,
        bidding: 14.17,
        write_medium: 14.17,
        write_hard: 14.17,
        write_heavy: 14.17,
        write_extreme: 14.17 do
    Q 'SELECT items.* FROM items WHERE items.id = ? -- 5'
    Q 'SELECT items.* FROM items WHERE items.quantity = ? -- 5_secondary'
    Q 'SELECT bids.* FROM items.bids WHERE items.id = ? -- 6'
    Q 'SELECT bids.* FROM items.bids WHERE items.quantity = ? -- 6_secondary'
  end

  Group 'SearchItemsByCategory', browsing: 27.77,
        bidding: 15.94,
        write_medium: 15.94,
        write_hard: 15.94,
        write_heavy: 15.94,
        write_extreme: 15.94 do
    Q 'SELECT items.id, items.name, items.initial_price, items.max_bid, items.nb_of_bids, items.end_date FROM items.category WHERE category.id = ? AND items.end_date >= ? LIMIT 25 -- 7'
    Q 'SELECT items.id, items.name, items.initial_price, items.max_bid, items.nb_of_bids, items.end_date FROM items.category WHERE category.dummy = ? AND items.end_date >= ? LIMIT 25 -- 7_secondary'
  end

  Group 'ViewUserInfo', browsing: 4.41,
        bidding: 2.48,
        write_medium: 2.48,
        write_hard: 2.48,
        write_heavy: 2.48,
        write_extreme: 2.48 do
    # XXX Not including region name below
    Q 'SELECT users.* FROM users WHERE users.id = ? -- 8'
    Q 'SELECT users.* FROM users WHERE users.rating = ? -- 8_secondary'
    Q 'SELECT comments.id, comments.rating, comments.date, comments.comment FROM comments.to_user WHERE to_user.id = ? -- 9'
    Q 'SELECT comments.id, comments.rating, comments.date, comments.comment FROM comments.to_user WHERE to_user.rating = ? -- 9_secondary'
  end

  Group 'RegisterItem', bidding: 0.53,
        write_medium: 0.53 * 10,
        write_hard: 0.53 * 50,
        write_heavy: 0.53 * 100,
        write_extreme: 0.53 * 200 do
    Q 'INSERT INTO items SET id=?, name=?, description=?, initial_price=?, quantity=?, reserve_price=?, buy_now=?, nb_of_bids=0, max_bid=0, start_date=?, end_date=? AND CONNECT TO category(?), seller(?) -- 10'
  end

  Group 'RegisterUser', bidding: 1.07,
        write_medium: 1.07 * 10,
        write_hard: 1.07 * 50,
        write_heavy: 1.07 * 100,
        write_extreme: 1.07 * 200 do
    Q 'INSERT INTO users SET id=?, firstname=?, lastname=?, nickname=?, ' \
      'password=?, email=?, rating=0, balance=0, creation_date=? ' \
      'AND CONNECT TO region(?) -- 11'
  end

  Group 'BuyNow', bidding: 1.16,
        write_medium: 1.16,
        write_hard: 1.16,
        write_heavy: 1.16,
        write_extreme: 1.16 do
    Q 'SELECT users.nickname FROM users WHERE users.id=? -- 12'
    Q 'SELECT users.nickname FROM users WHERE users.rating=? -- 12_secondary'
    Q 'SELECT items.* FROM items WHERE items.id=? -- 13'
    Q 'SELECT items.* FROM items WHERE items.quantity=? -- 13_secondary'
  end

  Group 'StoreBuyNow', bidding: 1.10,
        write_medium: 1.10 * 10,
        write_hard: 1.10 * 50,
        write_heavy: 1.10 * 100,
        write_extreme: 1.10 * 200 do
    Q 'SELECT items.quantity, items.nb_of_bids, items.end_date FROM items WHERE items.id=? -- 14'
    Q 'SELECT items.quantity, items.nb_of_bids, items.end_date FROM items WHERE items.quantity=? -- 14_secondary'
    Q 'UPDATE items SET quantity=?, nb_of_bids=?, end_date=? WHERE items.id=? -- 15'
    Q 'INSERT INTO buynow SET id=?, qty=?, date=? ' \
      'AND CONNECT TO item(?), buyer(?) -- 16'
  end

  Group 'PutBid', bidding: 5.40,
        write_medium: 5.40,
        write_hard: 5.40,
        write_heavy: 5.40,
        write_extreme: 5.40 do
    Q 'SELECT users.nickname, users.password FROM users WHERE users.id=? -- 17'
    Q 'SELECT users.nickname, users.password FROM users WHERE users.rating=? -- 17_secondary'
    Q 'SELECT items.* FROM items WHERE items.id=? -- 18'
    Q 'SELECT items.* FROM items WHERE items.quantity=? -- 18_secondary'
    Q 'SELECT bids.qty, bids.date FROM bids.item WHERE item.id=? ' \
      'LIMIT 2 -- 19'
    Q 'SELECT bids.qty, bids.date FROM bids.item WHERE item.quantity=? ' \
      'LIMIT 2 -- 19_secondary'
    #Q 'SELECT bids.qty, bids.date FROM bids.item WHERE item.quantity=? -- 19_secondary'
  end

  Group 'StoreBid', bidding: 3.74,
        write_medium: 3.74 * 10,
        write_hard: 3.74 * 50,
        write_heavy: 3.74 * 100,
        write_extreme: 3.74 * 200 do
    Q 'INSERT INTO bids SET id=?, qty=?, bid=?, date=? ' \
      'AND CONNECT TO item(?), user(?) -- 20'
    Q 'SELECT items.nb_of_bids, items.max_bid FROM items WHERE items.id=? -- 21'
    Q 'SELECT items.nb_of_bids, items.max_bid FROM items WHERE items.quantity=? -- 21_secondary'
    Q 'UPDATE items SET nb_of_bids=?, max_bid=? WHERE items.id=? -- 22'
  end

  Group 'PutComment', bidding: 0.46,
        write_medium: 0.46,
        write_hard: 0.46,
        write_heavy: 0.46,
        write_extreme: 0.46 do
    Q 'SELECT users.nickname, users.password FROM users WHERE users.id=? -- 23'
    Q 'SELECT users.nickname, users.password FROM users WHERE users.rating=? -- 23_secondary'
    Q 'SELECT items.* FROM items WHERE items.id=? -- 24'
    Q 'SELECT items.* FROM items WHERE items.quantity=? -- 24_secondary'
    Q 'SELECT users.* FROM users WHERE users.id=? -- 25'
    Q 'SELECT users.* FROM users WHERE users.rating=? -- 25_secondary'
  end

  Group 'StoreComment', bidding: 0.45,
        write_medium: 0.45 * 10,
        write_hard: 0.45 * 50,
        write_heavy: 0.45 * 100,
        write_extreme: 0.45 * 200 do
    Q 'SELECT users.rating FROM users WHERE users.id=? -- 26'
    Q 'SELECT users.rating FROM users WHERE users.rating=? -- 26_secondary'
    Q 'UPDATE users SET rating=? WHERE users.id=? -- 27'
    Q 'INSERT INTO comments SET id=?, rating=?, date=?, comment=? ' \
      'AND CONNECT TO to_user(?), from_user(?), item(?) -- 28'
  end

  Group 'AboutMe', bidding: 1.71,
        write_medium: 1.71,
        write_hard: 1.71,
        write_heavy: 1.71,
        write_extreme: 1.71 do
    Q 'SELECT users.* FROM users WHERE users.id=? -- 29'
    Q 'SELECT users.* FROM users WHERE users.rating=? -- 29_secondary'
    Q 'SELECT comments_received.* FROM users.comments_received WHERE users.id = ? -- 30'
    Q 'SELECT comments_received.* FROM users.comments_received WHERE users.rating = ? -- 30_secondary'
    Q 'SELECT from_user.nickname FROM comments.from_user WHERE comments.id = ? -- 31'
    Q 'SELECT from_user.nickname FROM comments.from_user WHERE comments.rating = ? -- 31_secondary'
    Q 'SELECT bought_now.*, items.* FROM items.bought_now.buyer WHERE buyer.id = ? AND bought_now.date>=? -- 32'
    Q 'SELECT bought_now.*, items.* FROM items.bought_now.buyer WHERE buyer.rating = ? AND bought_now.date>=? -- 32_secondary'
    Q 'SELECT items.* FROM items.seller WHERE seller.id=? AND items.end_date >=? -- 33'
    Q 'SELECT items.* FROM items.seller WHERE seller.rating =? AND items.end_date >=? -- 33_secondary'
    Q 'SELECT items.* FROM items.bids.user WHERE user.id=? AND items.end_date>=? -- 34'
    Q 'SELECT items.* FROM items.bids.user WHERE user.rating =? AND items.end_date>=? -- 34_secondary'
  end

  Group 'SearchItemsByRegion', browsing: 8.26,
        bidding: 6.34,
        write_medium: 6.34,
        write_hard: 6.34,
        write_heavy: 6.34,
        write_extreme: 6.34 do
    Q 'SELECT items.id, items.name, items.initial_price, items.max_bid, items.nb_of_bids, items.end_date FROM items.seller WHERE seller.region.id = ? AND items.category.id = ? AND items.end_date >= ? LIMIT 25 -- 35'
    #Q 'SELECT items.id, items.name, items.initial_price, items.max_bid, items.nb_of_bids, items.end_date FROM items.seller WHERE seller.region.dummy = ? AND items.category.id = ? AND items.end_date >= ? LIMIT 25 -- 35_secondary'
  end

  Group 'BrowseRegions', browsing: 3.21,
        bidding: 5.39,
        write_medium: 5.39,
        write_hard: 5.39,
        write_heavy: 5.39,
        write_extreme: 5.39 do
    # XXX Must have at least one equality predicate
    Q 'SELECT regions.id, regions.name FROM regions WHERE regions.dummy = 1 -- 36'
    #Q 'SELECT regions.id, regions.name FROM regions WHERE regions.name = 1 -- 36_secondary'
  end
end
