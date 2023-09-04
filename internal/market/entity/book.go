package entity

import (
	"container/heap"
	"sync"
)

type Book struct {
	Orders        []*Order
	Transactions  []*Transaction
	OrdersChan    chan *Order // receive kafka orders (BUY and SELL)
	OrdersChanOut chan *Order //
	Wg            *sync.WaitGroup
}

func NewBook(ordersChan chan *Order, ordersChanOut chan *Order, wg *sync.WaitGroup) *Book {
	return &Book{
		Orders:        []*Order{},
		Transactions:  []*Transaction{},
		OrdersChan:    ordersChan,
		OrdersChanOut: ordersChanOut,
		Wg:            wg,
	}
}

func (b *Book) Trade() {
	buyOrders := NewOrderQueue()
	sellOrders := NewOrderQueue()
	heap.Init(buyOrders)
	heap.Init(sellOrders)

	for order := range b.OrdersChan {
		if order.OrderType == "BUY" { // If its a Buying Order
			buyOrders.Push(order) // Add it to Buying Orders stack
			/*
				Lets check if there's any selling order in sellOrders stack
				And if there's is any selling order that the price is lower or equal to the buying order price
			*/
			if existsOrderTypeOf(sellOrders) && priceLessOrEqualTo(sellOrders, order.Price) {
				// remove this found order from sell channel
				sellOrder := sellOrders.Pop().(*Order)
				if sellOrder.PendingShares > 0 { // Check if in this order theres any pending share to create transaction
					transaction := NewTransaction(sellOrder, order, order.Shares, order.Price) // create transaction
					b.AddTransaction(transaction, b.Wg)                                        // Add transaction to the book
					sellOrder.Transactions = append(sellOrder.Transactions, transaction)       // register transaction on the sellOrders stack
					order.Transactions = append(order.Transactions, transaction)               // register buy order on the buyOrders stack
					b.OrdersChanOut <- sellOrder
					b.OrdersChanOut <- order
					if sellOrder.PendingShares > 0 {
						sellOrders.Push(sellOrder)
					}
				}
			}
		} else if order.OrderType == "SELL" {
			sellOrders.Push(order)
			/*
				Lets check if there's any buying order in buyOrders stack
				And if there's is any buying order that the price is greater or equal to the selling order price
			*/
			if existsOrderTypeOf(buyOrders) && priceGreaterOrEqualTo(sellOrders, order.Price) {
				buyOrder := buyOrders.Pop().(*Order)
				if buyOrder.PendingShares > 0 { // Check if in this order theres any pending share to create transaction
					transaction := NewTransaction(buyOrder, order, order.Shares, order.Price)
					b.AddTransaction(transaction, b.Wg)
					buyOrder.Transactions = append(buyOrder.Transactions, transaction)
					order.Transactions = append(order.Transactions, transaction)
					b.OrdersChanOut <- buyOrder
					b.OrdersChanOut <- order
					if buyOrder.PendingShares > 0 {
						buyOrders.Push(buyOrder)
					}
				}
			}
		}
	}
}

func existsOrderTypeOf(orderQueue *OrderQueue) bool {
	return orderQueue.Len() > 0
}

func priceLessOrEqualTo(orderQueue *OrderQueue, price float64) bool {
	return orderQueue.Orders[0].Price <= price
}

func priceGreaterOrEqualTo(orderQueue *OrderQueue, price float64) bool {
	return orderQueue.Orders[0].Price >= price
}

func (b *Book) AddTransaction(transaction *Transaction, wg *sync.WaitGroup) {
	defer wg.Done()
	sellingShares := transaction.SellingOrder.PendingShares
	buyingShares := transaction.BuyingOrder.PendingShares

	minShares := sellingShares
	if buyingShares < minShares {
		minShares = buyingShares
	}
	transaction.SyncShares(minShares)
	transaction.CalculateTotal(transaction.Shares, transaction.BuyingOrder.Price)
	transaction.CloseOrdersWithZeroShares()
}
