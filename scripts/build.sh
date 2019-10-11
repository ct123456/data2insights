file="builds/dependencies.zip"

if [ -f $file ] ; then
    rm $file
fi

zip -r $file .