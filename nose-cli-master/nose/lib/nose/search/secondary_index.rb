#yusuke index系列を受け取って{column_family_id => has_index}のhashを返す関数
# yusuke extension作成用
class CreateIndex
  #yusuke 試しにsecondary indexに置き換えるかどうか判断するメソッドを作成してみる。
  # index.all_fields-index.hash_fields-index.order_fields の数が一定値以上なら置き換えるにしよう。ひとまず、今は1つ以上あれば置き換えるコードで。
  def is_replace_2nd_index(index)
    non_key_field_num_threshold = 0
    non_key_fields = index.all_fields.length - index.hash_fields.length - index.order_fields.length
    non_key_fields > non_key_field_num_threshold
  end

  def get_MySQL_table_name_by_index(index)
    index.hash_fields.first.to_s.split('.').first
  end

  def external(indexes)
    indexes = indexes.to_a.sort_by do |i| #yusuke ここでtable名でsortし、それぞれのtable名の中でfieldの数が降順に並ぶようにする
      [get_MySQL_table_name_by_index(i),i.all_fields.length]
    end.reverse!
    table_name = ""
    fields = Set.new
    has_index_array =  indexes.map do |index|
      mySQL_table_name = get_MySQL_table_name_by_index(index)
      next HasIndex.new(index.key,false,nil) if !is_replace_2nd_index index #yusuke 単体のindexとして置き換えるメリットが無い場合、弾く

      #yusuke 作成するcolumn familyが初めてのテーブルに対するものならindexは作成しないので返す
      if table_name != mySQL_table_name then
        fields = index.all_fields
        table_name = mySQL_table_name
        next HasIndex.new(index.key,false,nil)
      end

      #yusuke 同じテーブル名を持ち、fieldsがfield数最大のものの部分集合でない場合indexを作成しない
      if !fields.superset? index.all_fields then
        next HasIndex.new(index.key, false,nil)
      end

      #secondary indexを作成する際に、どのテーブルを実テーブルとして使用するか取得する
      parent_table_id = indexes.select{|i| get_MySQL_table_name_by_index(i) == mySQL_table_name }.first.key

      next HasIndex.new(index.key,true,parent_table_id) #yusuke 上記の条件を全てクリアした場合、index作成
    end
    has_index_array
  end

  #yusuke 内部拡張機能としてsecondary indexを作成するだけのmethod。indexのis_secondary_indexフラグのみを見てhashを作成する
  def internal(indexes)
    indexes.map do |index|
      HasIndex.new(index.key, index.is_secondary_index, index.base_cf_key)
    end
  end

  def get_has_index_hash(indexes)
    #return external(indexes)
    return internal(indexes)
  end
end


#indexがsecondary indexを持つかどうかを保持するためのclass
class HasIndex
  attr_accessor :index_key,:index_value,:parent_table_id

  def initialize(key,value,parent_table_id)
    @index_key =key
    @index_value = value
    @parent_table_id = parent_table_id
  end
end
