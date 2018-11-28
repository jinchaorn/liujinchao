object erfenfa {
  def main(args: Array[String]): Unit = {
    val array: Array[Int] = Array(1,2,3,4,5,6,7,8,9,40,58,67)
    var start=0
    var end=array.length-1
    var ve=5;
    while (start<=end){
      var middle=(end+start)/2
        if(ve<array(middle)){
            end=middle
        }else if(ve>array(middle)){
          start=middle
        }else{
          println(middle)
          return
        }


    }
  }

}
