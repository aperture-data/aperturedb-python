from __future__ import annotations
from PIL import Image,ImageDraw,ImageFont
import subprocess
import argparse
import re
import random
import hashlib
from colorsys import rgb_to_hsv,hsv_to_rgb

class OutputFilePath():
    def __init__(self,path):
        self.path = path
    def __str__(self):
        return f"{self.path}"
    def get_file(self,ext,number,zerofill=0):
        has_subst = False

        split = self.path.split("%%")
        stot = len(split)
        # ensure substituion and number are included.
        if stot >1:
            final = []
            for i in range(stot-1):
                final = final + [ split[i] , str(number).zfill(zerofill)]
            file = "".join( final+[split[stot-1]])
            if not self.path.endswith(ext):
                file = file + ext

        else:
            file = self.path
            if self.path.endswith(ext):
                file = self.path[:-len(ext)]
            file = {file} + str(number).zfill(zerofill) + ext

        return file

    @classmethod
    def parse(cls, input_str:str) -> OutputFilePath:
        return OutputFilePath(input_str)
    
class ImageSize():
    def __init__(self,width,height):
        self.width = width
        self.height = height
    def size(self):
        return (self.width,self.height)
    def __str__(self):
        return f"{self.width}x{self.height}"
    @classmethod
    def parse(cls, input_str:str) -> ImageSize:
        width = 0
        height = 0
        fmt = re.compile("(^\d+)\s*(x|X)\s*(\d+)$")

        try:
            result = fmt.match(input_str)
            width = int(result.group(1))
            height = int(result.group(3))
        except:
            print("no")
            raise argparse.ArgumentTypeError(f"{input_str} is not a valid image size; requires width x height")

        return ImageSize(width,height)
            
fnt = None            
def load_preferred_font(name,size):
    global fnt
    if fnt is not None:
        return fnt
    try:
        fnt = ImageFont.truetype(name,size)
    except OSError:
        best_font = subprocess.run(["fc-match", "Sans"],stdout=subprocess.PIPE, text=True)
        if best_font.returncode != 0:
            raise Exception("Can't find a font")
        fc_output = best_font.stdout
        font = fc_output.split(":")[0]
        fnt = ImageFont.truetype(font,size)
    return fnt

def complementary_color(orig_color):
    hsv = rgb_to_hsv(*orig_color)
    # rotate hue and invert value
    return tuple( int(v) for v in hsv_to_rgb( ( hsv[0] + 0.5 ) %1,hsv[1], abs(1-hsv[2])))

write_log=[]
def draw_and_save(of,ot,os,text):
    global write_log
    # choose color based on hash output 
    bg_color = tuple( random.randint(0,255) for _ in range(3))
    fg_color = complementary_color( bg_color )
    color_white = (255,255,255,255)
    color_red = (255,0,0,255)
    canvas = Image.new("RGBA",os.size(),bg_color)
    fnt = load_preferred_font('DejaVuSans-Bold.ttf',16)
    d = ImageDraw.Draw(canvas)
    d.text((10,10),text,font=fnt,fill=fg_color)
    canvas.save(of,ot)
    write_log.append(of)
    print(f"* Saved {of} of size {os} of type {ot} with text {text}")

def getoptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--count',type=int,default=1)
    parser.add_argument('-s' ,'--size',type=ImageSize.parse, default=ImageSize(256,256))
    parser.add_argument('-o' ,'--output',type=OutputFilePath.parse, default=OutputFilePath("/tmp/generated_image%%"))
    parser.add_argument('-z' ,'--zerofill',type=int, default=0)
    parser.add_argument('-t' ,'--imagetype',choices=['jpg','png'],default='png')
    parser.add_argument('-m' ,'--manifest',type=str,default=None)
    return parser.parse_args()

if __name__ == '__main__':
    params = getoptions()
    images = "image" if params.count == 1 else "images"
    print(f"Creating {params.count} {images}")
    ext = "." + params.imagetype
    # set random based on filename, but stable between runs.
    random.seed(
            int(hashlib.md5(params.output.get_file(ext,0,params.zerofill).encode()).hexdigest(),16))
    for i in range(0,params.count):
        draw_and_save(params.output.get_file(ext,i,params.zerofill), params.imagetype, params.size,f"{i}")

    if params.manifest:
        print(f"Writing manifest to {params.manifest}")
        with open(params.manifest,"w") as fp:
            for line in write_log:
                fp.write(line+"\n")

