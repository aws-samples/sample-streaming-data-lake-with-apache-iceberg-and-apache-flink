package com.aws.samples.iceberg.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Order event representing e-commerce order transactions.
 * Used for demonstrating upsert operations and nested structures.
 */
public class OrderEvent extends BaseEvent {
    
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("order_id")
    private String orderId;
    
    @JsonProperty("customer_id")
    private String customerId;
    
    @JsonProperty("amount")
    private BigDecimal amount;
    
    @JsonProperty("currency")
    private String currency;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("items")
    private List<OrderItem> items;
    
    public OrderEvent() {
        super();
        this.items = new ArrayList<>();
    }
    
    public OrderEvent(String eventId, Instant eventTime, String region, LocalDate eventDate,
                      String orderId, String customerId, BigDecimal amount, String currency, String status) {
        super(eventId, eventTime, "ORDER", region, eventDate);
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.items = new ArrayList<>();
    }
    
    // Getters and Setters
    
    public String getOrderId() {
        return orderId;
    }
    
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    
    public String getCustomerId() {
        return customerId;
    }
    
    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }
    
    public BigDecimal getAmount() {
        return amount;
    }
    
    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
    
    public String getCurrency() {
        return currency;
    }
    
    public void setCurrency(String currency) {
        this.currency = currency;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public List<OrderItem> getItems() {
        return items;
    }
    
    public void setItems(List<OrderItem> items) {
        this.items = items;
    }
    
    public void addItem(OrderItem item) {
        this.items.add(item);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        OrderEvent that = (OrderEvent) o;
        return Objects.equals(orderId, that.orderId) &&
               Objects.equals(customerId, that.customerId) &&
               Objects.equals(amount, that.amount) &&
               Objects.equals(currency, that.currency) &&
               Objects.equals(status, that.status) &&
               Objects.equals(items, that.items);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), orderId, customerId, amount, currency, status, items);
    }
    
    @Override
    public String toString() {
        return "OrderEvent{" +
               "eventId='" + getEventId() + '\'' +
               ", eventTime=" + getEventTime() +
               ", orderId='" + orderId + '\'' +
               ", customerId='" + customerId + '\'' +
               ", amount=" + amount +
               ", currency='" + currency + '\'' +
               ", status='" + status + '\'' +
               ", region='" + getRegion() + '\'' +
               ", eventDate=" + getEventDate() +
               ", items=" + items +
               '}';
    }
    
    /**
     * Nested class representing an order item.
     */
    public static class OrderItem implements java.io.Serializable {
        
        private static final long serialVersionUID = 1L;
        
        @JsonProperty("product_id")
        private String productId;
        
        @JsonProperty("product_name")
        private String productName;
        
        @JsonProperty("quantity")
        private Integer quantity;
        
        @JsonProperty("price")
        private BigDecimal price;
        
        public OrderItem() {
        }
        
        public OrderItem(String productId, String productName, Integer quantity, BigDecimal price) {
            this.productId = productId;
            this.productName = productName;
            this.quantity = quantity;
            this.price = price;
        }
        
        public String getProductId() {
            return productId;
        }
        
        public void setProductId(String productId) {
            this.productId = productId;
        }
        
        public String getProductName() {
            return productName;
        }
        
        public void setProductName(String productName) {
            this.productName = productName;
        }
        
        public Integer getQuantity() {
            return quantity;
        }
        
        public void setQuantity(Integer quantity) {
            this.quantity = quantity;
        }
        
        public BigDecimal getPrice() {
            return price;
        }
        
        public void setPrice(BigDecimal price) {
            this.price = price;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderItem orderItem = (OrderItem) o;
            return Objects.equals(productId, orderItem.productId) &&
                   Objects.equals(productName, orderItem.productName) &&
                   Objects.equals(quantity, orderItem.quantity) &&
                   Objects.equals(price, orderItem.price);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(productId, productName, quantity, price);
        }
        
        @Override
        public String toString() {
            return "OrderItem{" +
                   "productId='" + productId + '\'' +
                   ", productName='" + productName + '\'' +
                   ", quantity=" + quantity +
                   ", price=" + price +
                   '}';
        }
    }
}
