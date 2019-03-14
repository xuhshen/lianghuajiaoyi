'''
Created on 2019年1月16日

@author: 04yyl
'''
from pyecharts import Line,Page,Scatter3D,Overlap
class Draw(object):
    def __init__(self,path,ispage=True):
        self.page = Page()
        self.overlap = Overlap()
        self.ispage = ispage
        self.path = path
        self.range_color = [
            "#313695",
            "#4575b4",
            "#74add1",
            "#abd9e9",
            "#e0f3f8",
            "#ffffbf",
            "#fee090",
            "#fdae61",
            "#f46d43",
            "#d73027",
            "#a50026",
        ]

    def add (self,name,xs,ys):
        line = Line(name)
        for k,v in ys.items():
            line.add(k, xs, v, is_smooth=False,is_datazoom_show=True,yaxis_min=min(v))
        self.page.add(line)
    
    def add3d(self,name,data):
        surface3D = Scatter3D(name, width=1200, height=600)
        surface3D.add(
            "",
            data,
            is_visualmap=True,
            visual_range=[-1000, 4000],
            visual_range_color=self.range_color,
        )
        self.page.add(surface3D)
    
    def add2y(self,name,xs,ys,yaxis_index=0):
        line = Line(name)
        for k,v in ys.items():
            line.add(k, xs, v, is_smooth=False,is_datazoom_show=True)
        self.overlap.add(line,yaxis_index=yaxis_index,is_add_yaxis=True)
        
         
    def draw(self):
        if self.ispage:
            self.page.render(self.path)
        else:
            self.overlap.render(self.path)
        
        
        