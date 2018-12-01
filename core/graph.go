package auccore

import (
	"image"
	"image/color"
	"image/draw"
	"image/gif"
	"os"
	"strconv"

	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"golang.org/x/image/font/gofont/gomonobold"
	"golang.org/x/image/font/gofont/goregular"
	"golang.org/x/image/math/fixed"
)

var (
	colorBgHeader        = image.NewUniform(color.RGBA{159, 159, 159, 0xff})
	colorBgContent       = image.NewUniform(color.RGBA{215, 215, 215, 0xff})
	colorBgContentOmit   = image.NewUniform(color.RGBA{231, 231, 231, 0xff})
	colorTextContentOmit = image.NewUniform(color.RGBA{255, 255, 255, 0xff})
)

// Graph take snapshot of store and save as animation git
type Graph struct {
	palette []color.Color
	font    *truetype.Font
	font2   *truetype.Font
	images  []*image.Paletted
	delays  []int

	frame *image.Paletted
}

func NewGraph() *Graph {
	f, _ := truetype.Parse(goregular.TTF)
	f2, _ := truetype.Parse(gomonobold.TTF)
	// Gif palette accept 256 colors
	var p []color.Color
	for i := 255; i >= 0; i-- {
		p = append(p, color.RGBA{uint8(i), uint8(i), uint8(i), 0xff})
	}

	return &Graph{
		font:    f,
		font2:   f2,
		palette: p,
	}
}

// Snapshot take an snapshot
func (g *Graph) Snapshot(st *Store) {
	g.createFrame()
	success := 0
	colIdx := 0
	for i, key := range st.PriceChain.Index {
		b := st.PriceChain.Blocks[key]

		colIdx = len(st.PriceChain.Index) - 1 - i
		g.appendChain(colIdx, b)
		//log.Printf("====Batch  %4d %6d %6d====\n", b.Key, b.Total, b.Valid)

		rowIdx := 0
		for e := b.Front(); e != nil; e = e.Next() {
			bid := e.Bid
			var mark = "" //  ✂ ✔ ✘
			if !bid.Active {
				mark = "✂"
			} else if success < st.Capacity {
				success++
				mark = "✔"
			} else {
				mark = "✘"
			}

			g.appendBlock(colIdx, rowIdx, bid, mark)
			rowIdx++
			//log.Printf("%s   %d  %4d    %s\n", bid.Time.Format("15:04:05.000000"), bid.Client, bid.Price, mark)
		}

	}

	g.saveFrame()
}

// Output output result file
func (g *Graph) Output() {
	// insert blink frame
	lastFrame := g.images[len(g.images)-1]
	emptyFrame := image.NewPaletted(image.Rect(0, 0, 1000, 500), g.palette)
	for i := 0; i < 3; i++ {
		g.images = append(g.images, emptyFrame)
		g.delays = append(g.delays, 20)

		g.images = append(g.images, lastFrame)
		g.delays = append(g.delays, 20)
	}

	// increase last frame
	g.delays[len(g.delays)-1] = 1000

	w, _ := os.Create("../logs/img.gif")
	defer w.Close()
	gif.EncodeAll(w, &gif.GIF{
		Image: g.images,
		Delay: g.delays,
	})
}

func (g *Graph) createFrame() {
	g.frame = image.NewPaletted(image.Rect(0, 0, 1000, 500), g.palette)

	ctx := freetype.NewContext()
	ctx.SetFont(g.font2)
	ctx.SetDst(g.frame)
	ctx.SetFontSize(16)
	ctx.SetClip(g.frame.Bounds())
	ctx.SetSrc(image.Black)
	point := fixed.P(65, 20)
	ctx.DrawString("Price ---→", point)

	point = fixed.P(10, 93)
	ctx.DrawString("Time", point)
	point = fixed.P(22, 110)
	ctx.DrawString("¦", point)
	point = fixed.P(22, 128)
	ctx.DrawString("¦", point)
	point = fixed.P(22, 146)
	ctx.DrawString("↓", point)
}

func (g *Graph) saveFrame() {
	g.images = append(g.images, g.frame)
	g.delays = append(g.delays, 300)
}

func (g *Graph) appendChain(col int, b *Block) {
	x := image.NewRGBA(image.Rect(0, 0, 80, 40))
	draw.Draw(x, x.Rect, colorBgHeader, image.ZP, draw.Over)
	rd := image.Point{col*80 + (col+1)*5 + 60, 25}
	rr := image.Rectangle{rd, rd.Add(x.Bounds().Size())}
	draw.Draw(g.frame, rr, x, g.frame.Bounds().Min, draw.Over)

	ctx := freetype.NewContext()
	ctx.SetFont(g.font)
	ctx.SetDst(g.frame)
	ctx.SetFontSize(16)
	ctx.SetClip(g.frame.Bounds())
	ctx.SetSrc(image.Black)
	point := fixed.P(col*80+(col+1)*5+60+25, 50)
	ctx.DrawString(""+strconv.Itoa(b.Key), point)
}

func (g *Graph) appendBlock(col, row int, b *Bid, mark string) {
	x := image.NewRGBA(image.Rect(0, 0, 80, 30))
	if !b.Active {
		draw.Draw(x, x.Rect, colorBgContentOmit, image.ZP, draw.Over)
	} else {
		draw.Draw(x, x.Rect, colorBgContent, image.ZP, draw.Over)
	}
	rd := image.Point{col*80 + (col+1)*5 + 60, row*30 + (row+1)*3 + 70}
	rr := image.Rectangle{rd, rd.Add(x.Bounds().Size())}
	draw.Draw(g.frame, rr, x, g.frame.Bounds().Min, draw.Over)

	ctx := freetype.NewContext()

	ctx.SetDst(g.frame)
	ctx.SetFontSize(9)
	ctx.SetClip(g.frame.Bounds())
	if !b.Active {
		ctx.SetSrc(colorTextContentOmit)
	} else {
		ctx.SetSrc(image.Black)
	}
	if mark == "✔" {
		ctx.SetFont(g.font2)
	} else {
		ctx.SetFont(g.font)
	}
	point := fixed.P(col*80+(col+1)*5+60+3, row*30+(row+1)*3+70+20)
	ctx.DrawString(strconv.Itoa(b.Client)+" @ "+b.Time.Format("15:04:05"), point)
}
