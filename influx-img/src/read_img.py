from PIL import Image
import numpy as np
import matplotlib.pyplot as plt

image = Image.open("./img/img_3.jpeg")  # 用PIL中的Image.open打开图像
image_arr1 = np.array(image)  # 转化成numpy数组
# print(image_arr1)
# print(image_arr1.shape)
image = Image.open("./img/img_4.jpeg")  # 用PIL中的Image.open打开图像
image_arr2 = np.array(image)  # 转化成numpy数组
# print(image_arr2)
# print(image_arr2.shape)
# print(image_arr1 == image_arr2)
# for i in image_arr1 == image_arr2:
#     print(i)
# img = plt.imread("./img/img_3.jpeg")
# print(img)
# img = plt.imread("./img/img_4.jpeg")
# print(img)

# image = Image.open("./img/img_4.png")
# image_arr3 = np.array(image)
# print(image_arr3)
# print(image_arr3.shape)
image = Image.open("./img/img_5.png")
image_arr3 = np.array(image)
print(image_arr3)
print(image_arr3.shape)

image = Image.open("./img/img_6.jpeg")
image_arr3 = np.array(image)
print(image_arr3)
print(image_arr3.shape)
image.close()
















