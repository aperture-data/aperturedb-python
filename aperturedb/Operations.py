class Operations(object):

    def __init__(self):

        self.operations_arr = []

    def get_operations_arr(self):
        return self.operations_arr

    def resize(self, width, height):

        op = {
            "type": "resize",
            "width":  width,
            "height": height,
        }

        self.operations_arr.append(op)

    def rotate(self, angle, resize=False):

        op = {
            "type": "rotate",
            "angle": angle,
            "resize": resize,
        }

        self.operations_arr.append(op)
